package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/mxmCherry/openrtb"
	"github.com/prebid/prebid-server/adapters"
	"github.com/prebid/prebid-server/errortypes"
	"github.com/prebid/prebid-server/openrtb_ext"
	"golang.org/x/net/context/ctxhttp"
)

// AdaptedBidder defines the contract needed to participate in an Auction within an Exchange.
//
// This interface exists to help segregate core auction logic.
//
// Any logic which can be done _within a single Seat_ goes inside one of these.
// Any logic which _requires responses from all Seats_ goes inside the Exchange.
//
// This interface differs from adapters.Bidder to help minimize code duplication across the
// adapters.Bidder implementations.
type AdaptedBidder interface {
	// RequestBid fetches bids for the given request.
	//
	// An adaptedBidder *may* return two non-nil values here. Errors should describe situations which
	// make the bid (or no-bid) "less than ideal." Common examples include:
	//
	// 1. Connection issues.
	// 2. Imps with Media Types which this Bidder doesn't support.
	// 3. The Context timeout expired before all expected bids were returned.
	// 4. The Server sent back an unexpected Response, so some bids were ignored.
	//
	// Any errors will be user-facing in the API.
	// Error messages should help publishers understand what might account for "bad" bids.
	RequestBid(ctx context.Context, request *openrtb.BidRequest, name openrtb_ext.BidderName, bidAdjustment float64) (*PBSOrtbSeatBid, []error)
}

// PBSOrtbBid is a Bid returned by an adaptedBidder.
//
// PBSOrtbBid.bid.Ext will become "response.seatbid[i].bid.ext.bidder" in the final OpenRTB response.
// PBSOrtbBid.bidType will become "response.seatbid[i].bid.ext.prebid.type" in the final OpenRTB response.
// PBSOrtbBid.bidTargets does not need to be filled out by the Bidder. It will be set later by the exchange.
type PBSOrtbBid struct {
	Bid        *openrtb.Bid
	BidType    openrtb_ext.BidType
	BidTargets map[string]string
}

// PBSOrtbSeatBid is a SeatBid returned by an adaptedBidder.
//
// This is distinct from the openrtb.SeatBid so that the prebid-server ext can be passed back with typesafety.
type PBSOrtbSeatBid struct {
	// Bids is the list of Bids which this adaptedBidder wishes to make.
	Bids []*PBSOrtbBid
	// Currency is the Currency in which the bids are made.
	// Should be a valid curreny ISO code.
	Currency string
	// HTTPCalls is the list of debugging info. It should only be populated if the request.test == 1.
	// This will become response.ext.debug.httpcalls.{bidder} on the final Response.
	HTTPCalls []*openrtb_ext.ExtHttpCall
	// Ext contains the extension for this seatbid.
	// if len(bids) > 0, this will become response.seatbid[i].Ext.{bidder} on the final OpenRTB response.
	// if len(bids) == 0, this will be ignored because the OpenRTB spec doesn't allow a SeatBid with 0 Bids.
	Ext json.RawMessage
}

// AdaptBidder converts an adapters.Bidder into an exchange.adaptedBidder.
//
// The name refers to the "Adapter" architecture pattern, and should not be confused with a Prebid "Adapter"
// (which is being phased out and replaced by Bidder for OpenRTB auctions)
func AdaptBidder(bidder adapters.Bidder, client *http.Client) AdaptedBidder {
	return &BidderAdapter{
		Bidder: bidder,
		Client: client,
	}
}

type BidderAdapter struct {
	Bidder adapters.Bidder
	Client *http.Client
}

func (bidder *BidderAdapter) RequestBid(ctx context.Context, request *openrtb.BidRequest, name openrtb_ext.BidderName, bidAdjustment float64) (*PBSOrtbSeatBid, []error) {
	reqData, errs := bidder.Bidder.MakeRequests(request)

	if len(reqData) == 0 {
		// If the adapter failed to generate both requests and errors, this is an error.
		if len(errs) == 0 {
			errs = append(errs, &errortypes.FailedToRequestBids{Message: "The adapter failed to generate any bid requests, but also failed to generate an error explaining why"})
		}
		return nil, errs
	}

	// Make any HTTP requests in parallel.
	// If the bidder only needs to make one, save some cycles by just using the current one.
	responseChannel := make(chan *httpCallInfo, len(reqData))
	if len(reqData) == 1 {
		responseChannel <- bidder.doRequest(ctx, reqData[0])
	} else {
		for _, oneReqData := range reqData {
			go func(data *adapters.RequestData) {
				responseChannel <- bidder.doRequest(ctx, data)
			}(oneReqData) // Method arg avoids a race condition on oneReqData
		}
	}

	seatBid := &PBSOrtbSeatBid{
		Bids:      make([]*PBSOrtbBid, 0, len(reqData)),
		Currency:  "USD",
		HTTPCalls: make([]*openrtb_ext.ExtHttpCall, 0, len(reqData)),
	}

	firstHTTPCallCurrency := ""

	// If the bidder made multiple requests, we still want them to enter as many bids as possible...
	// even if the timeout occurs sometime halfway through.
	for i := 0; i < len(reqData); i++ {
		httpInfo := <-responseChannel
		// If this is a test bid, capture debugging info from the requests.
		if request.Test == 1 {
			seatBid.HTTPCalls = append(seatBid.HTTPCalls, makeExt(httpInfo))
		}

		if httpInfo.err == nil {

			bidResponse, moreErrs := bidder.Bidder.MakeBids(request, httpInfo.request, httpInfo.response)
			errs = append(errs, moreErrs...)

			if bidResponse != nil {

				if bidResponse.Currency == "" {
					bidResponse.Currency = "USD"
				}

				// Related to #281 - currency support
				// Prebid can't make sure that each HTTP call returns bids with the same currency as the others.
				// If a Bidder makes two HTTP calls, and their servers respond with different currencies,
				// we will consider the first call currency as standard currency and then reject others which contradict it.
				if firstHTTPCallCurrency == "" { // First HTTP call
					firstHTTPCallCurrency = bidResponse.Currency
				}

				// TODO: #281 - Once currencies rate conversion is out, this shouldn't be an issue anymore, we will only
				// need to convert the bid price based on the currency.
				if firstHTTPCallCurrency == bidResponse.Currency {
					for i := 0; i < len(bidResponse.Bids); i++ {
						if bidResponse.Bids[i].Bid != nil {
							// TODO #280: Convert the bid price
							bidResponse.Bids[i].Bid.Price = bidResponse.Bids[i].Bid.Price * bidAdjustment
						}
						seatBid.Bids = append(seatBid.Bids, &PBSOrtbBid{
							Bid:     bidResponse.Bids[i].Bid,
							BidType: bidResponse.Bids[i].BidType,
						})
					}
				} else {
					errs = append(errs, fmt.Errorf(
						"Bid currencies mistmatch found. Expected all bids to have the same currencies. Expected '%s', was: '%s'",
						firstHTTPCallCurrency,
						bidResponse.Currency,
					))
				}
			}
		} else {
			errs = append(errs, httpInfo.err)
		}
	}

	return seatBid, errs
}

// makeExt transforms information about the HTTP call into the contract class for the PBS response.
func makeExt(httpInfo *httpCallInfo) *openrtb_ext.ExtHttpCall {
	if httpInfo.err == nil {
		return &openrtb_ext.ExtHttpCall{
			Uri:          httpInfo.request.Uri,
			RequestBody:  string(httpInfo.request.Body),
			ResponseBody: string(httpInfo.response.Body),
			Status:       httpInfo.response.StatusCode,
		}
	} else if httpInfo.request == nil {
		return &openrtb_ext.ExtHttpCall{}
	} else {
		return &openrtb_ext.ExtHttpCall{
			Uri:         httpInfo.request.Uri,
			RequestBody: string(httpInfo.request.Body),
		}
	}
}

// doRequest makes a request, handles the response, and returns the data needed by the
// Bidder interface.
func (bidder *BidderAdapter) doRequest(ctx context.Context, req *adapters.RequestData) *httpCallInfo {
	httpReq, err := http.NewRequest(req.Method, req.Uri, bytes.NewBuffer(req.Body))
	if err != nil {
		return &httpCallInfo{
			request: req,
			err:     err,
		}
	}
	httpReq.Header = req.Headers

	httpResp, err := ctxhttp.Do(ctx, bidder.Client, httpReq)
	if err != nil {
		if err == context.DeadlineExceeded {
			err = &errortypes.Timeout{Message: err.Error()}
		}
		return &httpCallInfo{
			request: req,
			err:     err,
		}
	}

	respBody, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return &httpCallInfo{
			request: req,
			err:     err,
		}
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 400 {
		err = &errortypes.BadServerResponse{
			Message: fmt.Sprintf("Server responded with failure status: %d. Set request.test = 1 for debugging info.", httpResp.StatusCode),
		}
	}

	return &httpCallInfo{
		request: req,
		response: &adapters.ResponseData{
			StatusCode: httpResp.StatusCode,
			Body:       respBody,
			Headers:    httpResp.Header,
		},
		err: err,
	}
}

type httpCallInfo struct {
	request  *adapters.RequestData
	response *adapters.ResponseData
	err      error
}
