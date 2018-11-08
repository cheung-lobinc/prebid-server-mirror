package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/golang/glog"
	"golang.org/x/text/currency"

	"github.com/mxmCherry/openrtb"

	"github.com/prebid/prebid-server/adapters"
	"github.com/prebid/prebid-server/config"
	"github.com/prebid/prebid-server/errortypes"
	"github.com/prebid/prebid-server/gdpr"
	"github.com/prebid/prebid-server/openrtb_ext"
	"github.com/prebid/prebid-server/pbsmetrics"
	"github.com/prebid/prebid-server/prebid_cache_client"
)

// Exchange runs Auctions. Implementations must be threadsafe, and will be shared across many goroutines.
type Exchange interface {
	// HoldAuction executes an OpenRTB v2.5 Auction.
	HoldAuction(ctx context.Context, bidRequest *openrtb.BidRequest, usersyncs IdFetcher, labels pbsmetrics.Labels) (*openrtb.BidResponse, error)
}

// IdFetcher can find the user's ID for a specific Bidder.
type IdFetcher interface {
	// GetId returns the ID for the bidder. The boolean will be true if the ID exists, and false otherwise.
	GetId(bidder openrtb_ext.BidderName) (string, bool)
}

type exchange struct {
	adapterMap          map[openrtb_ext.BidderName]AdaptedBidder
	me                  pbsmetrics.MetricsEngine
	cache               prebid_cache_client.Client
	cacheTime           time.Duration
	gDPR                gdpr.Permissions
	UsersyncIfAmbiguous bool
	defaultTTLs         config.DefaultTTLs
}

// Container to pass out response ext data from the GetAllBids goroutines back into the main thread
type SeatResponseExtra struct {
	ResponseTimeMillis int
	Errors             []openrtb_ext.ExtBidderError
}

type BidResponseWrapper struct {
	AdapterBids  *PBSOrtbSeatBid
	AdapterExtra *SeatResponseExtra
	Bidder       openrtb_ext.BidderName
}

func NewExchange(client *http.Client, cache prebid_cache_client.Client, cfg *config.Configuration, metricsEngine pbsmetrics.MetricsEngine, infos adapters.BidderInfos, gDPR gdpr.Permissions) Exchange {
	e := new(exchange)

	e.adapterMap = newAdapterMap(client, cfg, infos)
	e.cache = cache
	e.cacheTime = time.Duration(cfg.CacheURL.ExpectedTimeMillis) * time.Millisecond
	e.me = metricsEngine
	e.gDPR = gDPR
	e.UsersyncIfAmbiguous = cfg.GDPR.UsersyncIfAmbiguous
	e.defaultTTLs = cfg.CacheURL.DefaultTTLs
	return e
}

func (e *exchange) HoldAuction(ctx context.Context, bidRequest *openrtb.BidRequest, usersyncs IdFetcher, labels pbsmetrics.Labels) (*openrtb.BidResponse, error) {
	// Snapshot of resolved bid request for debug if test request
	var resolvedRequest json.RawMessage
	if bidRequest.Test == 1 {
		if r, err := json.Marshal(bidRequest); err != nil {
			glog.Errorf("Error marshalling bid request for debug: %v", err)
		} else {
			resolvedRequest = r
		}
	}

	// Slice of BidRequests, each a copy of the original cleaned to only contain bidder data for the named bidder
	blabels := make(map[openrtb_ext.BidderName]*pbsmetrics.AdapterLabels)
	cleanRequests, aliases, errs := CleanOpenRTBRequests(ctx, bidRequest, usersyncs, blabels, labels, e.gDPR, e.UsersyncIfAmbiguous)

	// List of bidders we have requests for.
	liveAdapters := make([]openrtb_ext.BidderName, len(cleanRequests))
	i := 0
	for a := range cleanRequests {
		liveAdapters[i] = a
		i++
	}
	// Randomize the list of adapters to make the auction more fair
	RandomizeList(liveAdapters)
	// Process the request to check for targeting parameters.
	var targData *TargetData
	shouldCacheBids := false
	shouldCacheVAST := false
	var bidAdjustmentFactors map[string]float64
	if len(bidRequest.Ext) > 0 {
		var requestExt openrtb_ext.ExtRequest
		err := json.Unmarshal(bidRequest.Ext, &requestExt)
		if err != nil {
			return nil, fmt.Errorf("Error decoding Request.ext : %s", err.Error())
		}
		bidAdjustmentFactors = requestExt.Prebid.BidAdjustmentFactors
		if requestExt.Prebid.Cache != nil {
			shouldCacheBids = requestExt.Prebid.Cache.Bids != nil
			shouldCacheVAST = requestExt.Prebid.Cache.VastXML != nil
		}

		if requestExt.Prebid.Targeting != nil {
			targData = &TargetData{
				PriceGranularity:  requestExt.Prebid.Targeting.PriceGranularity,
				IncludeWinners:    requestExt.Prebid.Targeting.IncludeWinners,
				IncludeBidderKeys: requestExt.Prebid.Targeting.IncludeBidderKeys,
			}
			if shouldCacheBids {
				targData.IncludeCacheBids = true
			}
			if shouldCacheVAST {
				targData.IncludeCacheVast = true
			}
		}
	}

	// If we need to cache bids, then it will take some time to call prebid cache.
	// We should reduce the amount of time the bidders have, to compensate.
	auctionCtx, cancel := e.makeAuctionContext(ctx, shouldCacheBids)
	defer cancel()

	adapterBids, adapterExtra := e.getAllBids(auctionCtx, cleanRequests, aliases, bidAdjustmentFactors, blabels)
	auc := NewAuction(adapterBids, len(bidRequest.Imp))
	if targData != nil {
		auc.SetRoundedPrices(targData.PriceGranularity)
		cacheErrs := auc.doCache(ctx, e.cache, targData.IncludeCacheBids, targData.IncludeCacheVast, bidRequest, 60, &e.defaultTTLs)
		if len(cacheErrs) > 0 {
			errs = append(errs, cacheErrs...)
		}
		targData.SetTargeting(auc, bidRequest.App != nil)
	}
	// Build the response
	return e.buildBidResponse(ctx, liveAdapters, adapterBids, bidRequest, resolvedRequest, adapterExtra, errs)
}

func (e *exchange) makeAuctionContext(ctx context.Context, needsCache bool) (auctionCtx context.Context, cancel func()) {
	auctionCtx = ctx
	cancel = func() {}
	if needsCache {
		if deadline, ok := ctx.Deadline(); ok {
			auctionCtx, cancel = context.WithDeadline(ctx, deadline.Add(-e.cacheTime))
		}
	}
	return
}

// This piece sends all the requests to the bidder adapters and gathers the results.
func (e *exchange) getAllBids(ctx context.Context, cleanRequests map[openrtb_ext.BidderName]*openrtb.BidRequest, aliases map[string]string, bidAdjustments map[string]float64, blabels map[openrtb_ext.BidderName]*pbsmetrics.AdapterLabels) (map[openrtb_ext.BidderName]*PBSOrtbSeatBid, map[openrtb_ext.BidderName]*SeatResponseExtra) {
	// Set up pointers to the bid results
	adapterBids := make(map[openrtb_ext.BidderName]*PBSOrtbSeatBid, len(cleanRequests))
	adapterExtra := make(map[openrtb_ext.BidderName]*SeatResponseExtra, len(cleanRequests))
	chBids := make(chan *BidResponseWrapper, len(cleanRequests))

	for bidderName, req := range cleanRequests {
		// Here we actually call the adapters and collect the bids.
		coreBidder := ResolveBidder(string(bidderName), aliases)
		bidderRunner := RecoverSafely(func(aName openrtb_ext.BidderName, coreBidder openrtb_ext.BidderName, request *openrtb.BidRequest, bidlabels *pbsmetrics.AdapterLabels) {
			// Passing in aName so a doesn't change out from under the go routine
			if bidlabels.Adapter == "" {
				glog.Errorf("Exchange: bidlables for %s (%s) missing adapter string", aName, coreBidder)
				bidlabels.Adapter = coreBidder
			}
			brw := new(BidResponseWrapper)
			brw.Bidder = aName
			// Defer basic metrics to insure we capture them after all the values have been set
			defer func() {
				e.me.RecordAdapterRequest(*bidlabels)
			}()
			start := time.Now()

			adjustmentFactor := 1.0
			if givenAdjustment, ok := bidAdjustments[string(aName)]; ok {
				adjustmentFactor = givenAdjustment
			}
			bids, err := e.adapterMap[coreBidder].RequestBid(ctx, request, aName, adjustmentFactor)

			// Add in time reporting
			elapsed := time.Since(start)
			brw.AdapterBids = bids
			// validate bids ASAP, so we don't waste time on invalid bids.
			err2 := brw.ValidateBids(request)
			if len(err2) > 0 {
				err = append(err, err2...)
			}
			// Structure to record extra tracking data generated during bidding
			ae := new(SeatResponseExtra)
			ae.ResponseTimeMillis = int(elapsed / time.Millisecond)
			// Timing statistics
			e.me.RecordAdapterTime(*bidlabels, time.Since(start))
			serr := ErrsToBidderErrors(err)
			bidlabels.AdapterBids = BidsToMetric(brw.AdapterBids)
			bidlabels.AdapterErrors = ErrorsToMetric(err)
			// Append any bid validation errors to the error list
			ae.Errors = serr
			brw.AdapterExtra = ae
			if bids != nil {
				for _, bid := range bids.Bids {
					var cpm = float64(bid.Bid.Price * 1000)
					e.me.RecordAdapterPrice(*bidlabels, cpm)
					e.me.RecordAdapterBidReceived(*bidlabels, bid.BidType, bid.Bid.AdM != "")
				}
			}
			chBids <- brw
		}, chBids)
		go bidderRunner(bidderName, coreBidder, req, blabels[coreBidder])
	}
	// Wait for the bidders to do their thing
	for i := 0; i < len(cleanRequests); i++ {
		brw := <-chBids
		adapterBids[brw.Bidder] = brw.AdapterBids
		adapterExtra[brw.Bidder] = brw.AdapterExtra
	}

	return adapterBids, adapterExtra
}

func RecoverSafely(inner func(openrtb_ext.BidderName, openrtb_ext.BidderName, *openrtb.BidRequest, *pbsmetrics.AdapterLabels), chBids chan *BidResponseWrapper) func(openrtb_ext.BidderName, openrtb_ext.BidderName, *openrtb.BidRequest, *pbsmetrics.AdapterLabels) {
	return func(aName openrtb_ext.BidderName, coreBidder openrtb_ext.BidderName, request *openrtb.BidRequest, bidlabels *pbsmetrics.AdapterLabels) {
		defer func() {
			if r := recover(); r != nil {
				glog.Errorf("OpenRTB auction recovered panic from Bidder %s: %v. Stack trace is: %v", coreBidder, r, string(debug.Stack()))
				// Let the master request know that there is no data here
				brw := new(BidResponseWrapper)
				brw.AdapterExtra = new(SeatResponseExtra)
				chBids <- brw
			}
		}()
		inner(aName, coreBidder, request, bidlabels)
	}
}

func BidsToMetric(bids *PBSOrtbSeatBid) pbsmetrics.AdapterBid {
	if bids == nil || len(bids.Bids) == 0 {
		return pbsmetrics.AdapterBidNone
	}
	return pbsmetrics.AdapterBidPresent
}

func ErrorsToMetric(errs []error) map[pbsmetrics.AdapterError]struct{} {
	if len(errs) == 0 {
		return nil
	}
	ret := make(map[pbsmetrics.AdapterError]struct{}, len(errs))
	var s struct{}
	for _, err := range errs {
		switch errortypes.DecodeError(err) {
		case errortypes.TimeoutCode:
			ret[pbsmetrics.AdapterErrorTimeout] = s
		case errortypes.BadInputCode:
			ret[pbsmetrics.AdapterErrorBadInput] = s
		case errortypes.BadServerResponseCode:
			ret[pbsmetrics.AdapterErrorBadServerResponse] = s
		case errortypes.FailedToRequestBidsCode:
			ret[pbsmetrics.AdapterErrorFailedToRequestBids] = s
		default:
			ret[pbsmetrics.AdapterErrorUnknown] = s
		}
	}
	return ret
}

func ErrsToBidderErrors(errs []error) []openrtb_ext.ExtBidderError {
	serr := make([]openrtb_ext.ExtBidderError, len(errs))
	for i := 0; i < len(errs); i++ {
		serr[i].Code = errortypes.DecodeError(errs[i])
		serr[i].Message = errs[i].Error()
	}
	return serr
}

// This piece takes all the bids supplied by the adapters and crafts an openRTB response to send back to the requester
func (e *exchange) buildBidResponse(ctx context.Context, liveAdapters []openrtb_ext.BidderName, adapterBids map[openrtb_ext.BidderName]*PBSOrtbSeatBid, bidRequest *openrtb.BidRequest, resolvedRequest json.RawMessage, adapterExtra map[openrtb_ext.BidderName]*SeatResponseExtra, errList []error) (*openrtb.BidResponse, error) {
	bidResponse := new(openrtb.BidResponse)

	bidResponse.ID = bidRequest.ID
	if len(liveAdapters) == 0 {
		// signal "Invalid Request" if no valid bidders.
		bidResponse.NBR = openrtb.NoBidReasonCode.Ptr(openrtb.NoBidReasonCodeInvalidRequest)
	}

	// Create the SeatBids. We use a zero sized slice so that we can append non-zero seat bids, and not include seatBid
	// objects for seatBids without any bids. Preallocate the max possible size to avoid reallocating the array as we go.
	seatBids := make([]openrtb.SeatBid, 0, len(liveAdapters))
	for _, a := range liveAdapters {
		if adapterBids[a] != nil && len(adapterBids[a].Bids) > 0 {
			sb := e.makeSeatBid(adapterBids[a], a, adapterExtra)
			seatBids = append(seatBids, *sb)
		}
	}

	bidResponse.SeatBid = seatBids

	bidResponseExt := e.makeExtBidResponse(adapterBids, adapterExtra, bidRequest, resolvedRequest, errList)
	ext, err := json.Marshal(bidResponseExt)
	bidResponse.Ext = ext
	return bidResponse, err
}

// Extract all the data from the SeatBids and build the ExtBidResponse
func (e *exchange) makeExtBidResponse(adapterBids map[openrtb_ext.BidderName]*PBSOrtbSeatBid, adapterExtra map[openrtb_ext.BidderName]*SeatResponseExtra, req *openrtb.BidRequest, resolvedRequest json.RawMessage, errList []error) *openrtb_ext.ExtBidResponse {
	bidResponseExt := &openrtb_ext.ExtBidResponse{
		Errors:             make(map[openrtb_ext.BidderName][]openrtb_ext.ExtBidderError, len(adapterBids)),
		ResponseTimeMillis: make(map[openrtb_ext.BidderName]int, len(adapterBids)),
	}
	if req.Test == 1 {
		bidResponseExt.Debug = &openrtb_ext.ExtResponseDebug{
			HttpCalls: make(map[openrtb_ext.BidderName][]*openrtb_ext.ExtHttpCall),
		}
		if err := json.Unmarshal(resolvedRequest, &bidResponseExt.Debug.ResolvedRequest); err != nil {
			glog.Errorf("Error unmarshalling bid request snapshot: %v", err)
		}
	}

	for a, b := range adapterBids {
		if b != nil {
			if req.Test == 1 {
				// Fill debug info
				bidResponseExt.Debug.HttpCalls[a] = b.HTTPCalls
			}
		}
		// Only make an entry for bidder errors if the bidder reported any.
		if len(adapterExtra[a].Errors) > 0 {
			bidResponseExt.Errors[a] = adapterExtra[a].Errors
		}
		if len(errList) > 0 {
			bidResponseExt.Errors["prebid"] = ErrsToBidderErrors(errList)
		}
		bidResponseExt.ResponseTimeMillis[a] = adapterExtra[a].ResponseTimeMillis
		// Defering the filling of bidResponseExt.Usersync[a] until later

	}
	return bidResponseExt
}

// Return an openrtb seatBid for a bidder
// BuildBidResponse is responsible for ensuring nil bid seatbids are not included
func (e *exchange) makeSeatBid(adapterBid *PBSOrtbSeatBid, adapter openrtb_ext.BidderName, adapterExtra map[openrtb_ext.BidderName]*SeatResponseExtra) *openrtb.SeatBid {
	seatBid := new(openrtb.SeatBid)
	seatBid.Seat = adapter.String()
	// Prebid cannot support roadblocking
	seatBid.Group = 0

	if len(adapterBid.Ext) > 0 {
		sbExt := ExtSeatBid{
			Bidder: adapterBid.Ext,
		}

		ext, err := json.Marshal(sbExt)
		if err != nil {
			extError := openrtb_ext.ExtBidderError{
				Code:    errortypes.DecodeError(err),
				Message: fmt.Sprintf("Error writing SeatBid.Ext: %s", err.Error()),
			}
			adapterExtra[adapter].Errors = append(adapterExtra[adapter].Errors, extError)
		}
		seatBid.Ext = ext
	}

	var errList []error
	seatBid.Bid, errList = e.makeBid(adapterBid.Bids, adapter)
	if len(errList) > 0 {
		adapterExtra[adapter].Errors = append(adapterExtra[adapter].Errors, ErrsToBidderErrors(errList)...)
	}

	return seatBid
}

// Create the Bid array inside of SeatBid
func (e *exchange) makeBid(Bids []*PBSOrtbBid, adapter openrtb_ext.BidderName) ([]openrtb.Bid, []error) {
	bids := make([]openrtb.Bid, 0, len(Bids))
	errList := make([]error, 0, 1)
	for _, thisBid := range Bids {
		bidExt := &openrtb_ext.ExtBid{
			Bidder: thisBid.Bid.Ext,
			Prebid: &openrtb_ext.ExtBidPrebid{
				Targeting: thisBid.BidTargets,
				Type:      thisBid.BidType,
			},
		}

		ext, err := json.Marshal(bidExt)
		if err != nil {
			errList = append(errList, err)
		} else {
			bids = append(bids, *thisBid.Bid)
			bids[len(bids)-1].Ext = ext
		}
	}
	return bids, errList
}

// ValidateBids will run some validation checks on the returned bids and excise any invalid bids
func (brw *BidResponseWrapper) ValidateBids(request *openrtb.BidRequest) (err []error) {
	// Exit early if there is nothing to do.
	if brw.AdapterBids == nil || len(brw.AdapterBids.Bids) == 0 {
		return
	}

	err = make([]error, 0, len(brw.AdapterBids.Bids))

	// By design, default currency is USD.
	if cerr := validateCurrency(request.Cur, brw.AdapterBids.Currency); cerr != nil {
		brw.AdapterBids.Bids = nil
		err = append(err, cerr)
		return
	}

	validBids := make([]*PBSOrtbBid, 0, len(brw.AdapterBids.Bids))
	for _, bid := range brw.AdapterBids.Bids {
		if ok, berr := validateBid(bid); ok {
			validBids = append(validBids, bid)
		} else {
			err = append(err, berr)
		}
	}
	if len(validBids) != len(brw.AdapterBids.Bids) {
		// If all bids are valid, the two slices should be equal. Otherwise replace the list of bids with the valid bids.
		brw.AdapterBids.Bids = validBids
	}
	return err
}

// validateCurrency will run currency validation checks and return true if it passes, false otherwise.
func validateCurrency(requestAllowedCurrencies []string, bidCurrency string) error {
	// Default currency is `USD` by design.
	defaultCurrency := "USD"
	// Make sure bid currency is a valid ISO currency code
	if bidCurrency == "" {
		// If bid currency is not set, then consider it's default currency.
		bidCurrency = defaultCurrency
	}
	currencyUnit, cerr := currency.ParseISO(bidCurrency)
	if cerr != nil {
		return cerr
	}
	// Make sure the bid currency is allowed from bid request via `cur` field.
	// If `cur` field array from bid request is empty, then consider it accepts the default currency.
	currencyAllowed := false
	if len(requestAllowedCurrencies) == 0 {
		requestAllowedCurrencies = []string{defaultCurrency}
	}
	for _, allowedCurrency := range requestAllowedCurrencies {
		if strings.ToUpper(allowedCurrency) == currencyUnit.String() {
			currencyAllowed = true
			break
		}
	}
	if currencyAllowed == false {
		return fmt.Errorf(
			"Bid currency is not allowed. Was '%s', wants: ['%s']",
			currencyUnit.String(),
			strings.Join(requestAllowedCurrencies, "', '"),
		)
	}

	return nil
}

// validateBid will run the supplied bid through validation checks and return true if it passes, false otherwise.
func validateBid(bid *PBSOrtbBid) (bool, error) {
	if bid.Bid == nil {
		return false, fmt.Errorf("Empty bid object submitted.")
	}
	// These are the three required fields for bids
	if bid.Bid.ID == "" {
		return false, fmt.Errorf("Bid missing required field 'id'")
	}
	if bid.Bid.ImpID == "" {
		return false, fmt.Errorf("Bid \"%s\" missing required field 'impid'", bid.Bid.ID)
	}
	if bid.Bid.Price <= 0.0 {
		return false, fmt.Errorf("Bid \"%s\" does not contain a positive 'price'", bid.Bid.ID)
	}
	if bid.Bid.CrID == "" {
		return false, fmt.Errorf("Bid \"%s\" missing creative ID", bid.Bid.ID)
	}

	return true, nil
}
