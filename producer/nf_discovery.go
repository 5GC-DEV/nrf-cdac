// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0

package producer

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/omec-project/nrf/context"
	"github.com/omec-project/nrf/dbadapter"
	"github.com/omec-project/nrf/logger"
	stats "github.com/omec-project/nrf/metrics"
	"github.com/omec-project/nrf/util"
	"github.com/omec-project/openapi/models"
	"github.com/omec-project/util/httpwrapper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	queryParamTargetNFType            = "target-nf-type"
	queryParamRequesterNFType         = "requester-nf-type"
	mongoOpExists                     = "$exists"
	queryParamServiceNames            = "service-names"
	mongoOpElemMatch                  = "$elemMatch"
	queryParamTargetPlmnList          = "target-plmn-list"
	queryParamTargetNfFqdn            = "target-nf-fqdn"
	queryParamNsiList                 = "nsi-list"
	queryParamSmfServingArea          = "smf-serving-area"
	errUnmarshalTaiByteArray          = "Unmarshal Error in taiByteArray: "
	queryParamAmfRegionID             = "amf-region-id"
	queryParamAmfSetID                = "amf-set-id"
	errUnmarshalGuamiByteArray        = "Unmarshal Error in guamiByteArray: "
	fieldUdmInfoSupiRanges            = "udmInfo.supiRanges"
	fieldUdmInfoGpsiRanges            = "udmInfo.gpsiRanges"
	fieldUdmExtGrpIDRanges            = "udmInfo.externalGroupIdentifiersRanges"
	fieldUdrInfoSupiRanges            = "udrInfo.supiRanges"
	fieldUdrInfoGpsiRanges            = "udrInfo.gpsiRanges"
	fieldUdrExtGroupIDRanges          = "udrInfo.externalGroupIdentifiersRanges"
	queryParamUeIpv4Address           = "ue-ipv4-address"
	queryParamIpDomain                = "ip-domain"
	queryParamUeIpv6Prefix            = "ue-ipv6-prefix"
	queryParamPgwInd                  = "pgw-ind"
	queryParamExternalGroupIdentity   = "external-group-identity"
	queryParamDataSet                 = "data-set"
	queryParamRoutingIndicator        = "routing-indicator"
	queryParamGroupIDList             = "group-id-list"
	queryParamDnaiList                = "dnai-list"
	queryParamUpfIwkEpsInd            = "upf-iwk-eps-ind"
	queryParamChfSupportedPlmn        = "chf-supported-plmn"
	fieldChfInfoPlmnRangeList         = "chfInfo.plmnRangeList"
	queryParamPreferredLocality       = "preferred-locality"
	queryParamAccessType              = "access-type"
	queryParamSupportedFeatures       = "supported-features"
	queryParamRequesterNfInstanceFqdn = "requester-nfinstance-fqdn"
	queryParamTargetNfInstanceID      = "target-nf-instanceid"
)

func HandleNFDiscoveryRequest(request *httpwrapper.Request) *httpwrapper.Response {
	// Get all query parameters
	// logger.DiscoveryLog.Infoln("Handle NFDiscoveryRequest")
	logger.DiscoveryLog.Infof("---Received NFDiscovery request: %v", request.Query)
	logger.DiscoveryLog.Infof("---Received NFDiscovery request: %s", request.Query.Encode())

	response, problemDetails := NFDiscoveryProcedure(request.Query)
	requesterNfType, targetNfType := GetRequesterAndTargetNfTypeGivenQueryParameters(request.Query)
	// Send Response
	// step 4: process the return value from step 3
	if response != nil {
		// status code is based on SPEC, and option headers
		stats.IncrementNrfNfInstancesStats(requesterNfType, targetNfType, "SUCCESS")
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementNrfNfInstancesStats(requesterNfType, targetNfType, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementNrfNfInstancesStats(requesterNfType, targetNfType, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func NFDiscoveryProcedure(queryParameters url.Values) (*models.SearchResult, *models.ProblemDetails) {
	if problem := validateMandatoryParams(queryParameters); problem != nil {
		return nil, problem
	}

	if problem := validateComplexQuery(queryParameters); problem != nil {
		return nil, problem
	}

	// Build Query Filter
	filter := buildFilter(queryParameters)
	logger.DiscoveryLog.Debugln("query filter:", filter)

	// Fetch NF Profiles
	nfProfilesRaw, _ := dbadapter.DBClient.RestfulAPIGetMany("NfProfile", filter)

	// Decode NF Profiles
	nfProfilesStruct := decodeNFProfiles(nfProfilesRaw)

	// Sort profiles
	sortNFProfiles(nfProfilesRaw)

	// Handle BSF IPv4/IPv6 conversion
	handleBSFIpConversion(queryParameters, nfProfilesStruct)

	// Build SearchResult
	searchResult := &models.SearchResult{
		ValidityPeriod: 100,
		NfInstances:    nfProfilesStruct,
	}

	return searchResult, nil
}

func validateMandatoryParams(queryParameters url.Values) *models.ProblemDetails {
	if queryParameters["target-nf-type"] == nil || queryParameters["requester-nf-type"] == nil {
		return &models.ProblemDetails{
			Title:  "Invalid Parameter",
			Status: http.StatusBadRequest,
			Cause:  "Loss mandatory parameter",
		}
	}

	return nil
}

func validateComplexQuery(queryParameters url.Values) *models.ProblemDetails {
	if queryParameters["complexQuery"] == nil {
		return nil
	}

	complexQuery := queryParameters["complexQuery"][0]

	complexQueryStruct := &models.ComplexQuery{}

	err := json.Unmarshal([]byte(complexQuery), complexQueryStruct)
	if err != nil {
		logger.DiscoveryLog.Warnln("UnMasrhal complexQuery Error: ", err)
	}

	if complexQueryStruct.CNf != nil && complexQueryStruct.DNf != nil {
		return &models.ProblemDetails{
			Title:  "Invalid Parameter",
			Status: http.StatusBadRequest,
			Cause:  "EITHER CNF OR DNF",
			InvalidParams: []models.InvalidParam{
				{Param: "complexQuery"},
			},
		}
	}

	return nil
}

func decodeNFProfiles(nfProfilesRaw []map[string]interface{}) []models.NfProfile {
	nfProfilesStruct, err := util.Decode(nfProfilesRaw, time.RFC3339)
	if err != nil {
		logger.DiscoveryLog.Warnln("NF Profile Raw decode error: ", nfProfilesStruct)
	}

	return nfProfilesStruct
}

func sortNFProfiles(nfProfilesRaw []map[string]interface{}) {
	sort.Slice(nfProfilesRaw, func(i, j int) bool {
		var updatedTimeVal time.Time
		if nfProfilesRaw[i]["expireAt"] == nil {
			return false
		}
		updatedTimeVal = nfProfilesRaw[j]["expireAt"].(primitive.DateTime).Time()

		return nfProfilesRaw[i]["expireAt"].(primitive.DateTime).Time().Before(updatedTimeVal)
	})
}

func handleBSFIpConversion(queryParameters url.Values, nfProfilesStruct []models.NfProfile) {
	if queryParameters["target-nf-type"][0] == "BSF" {
		for i, nfProfile := range nfProfilesStruct {
			if nfProfile.BsfInfo.Ipv4AddressRanges != nil {
				for j := range *nfProfile.BsfInfo.Ipv4AddressRanges {
					ipv4IntStart, err := strconv.Atoi((((*(*nfProfilesStruct[i].BsfInfo).Ipv4AddressRanges)[j]).Start))
					if err != nil {
						logger.DiscoveryLog.Warnln("ipv4IntStart Atoi Error: ", err)
					}
					((*(*nfProfilesStruct[i].BsfInfo).Ipv4AddressRanges)[j]).Start = context.Ipv4IntToIpv4String(int64(ipv4IntStart))
					ipv4IntEnd, err := strconv.Atoi((((*(*nfProfilesStruct[i].BsfInfo).Ipv4AddressRanges)[j]).End))
					if err != nil {
						logger.DiscoveryLog.Warnln("ipv4IntEnd Atoi Error: ", err)
					}
					((*(*nfProfilesStruct[i].BsfInfo).Ipv4AddressRanges)[j]).End = context.Ipv4IntToIpv4String(int64(ipv4IntEnd))
				}
			}
			if nfProfile.BsfInfo.Ipv6PrefixRanges != nil {
				for j := range *nfProfile.BsfInfo.Ipv6PrefixRanges {
					ipv6IntStart := new(big.Int)
					ipv6IntStart.SetString(((*(*nfProfilesStruct[i].BsfInfo).Ipv6PrefixRanges)[j]).Start, 10)
					((*(*nfProfilesStruct[i].BsfInfo).Ipv6PrefixRanges)[j]).Start = context.Ipv6IntToIpv6String(ipv6IntStart)

					ipv6IntEnd := new(big.Int)
					ipv6IntEnd.SetString(((*(*nfProfilesStruct[i].BsfInfo).Ipv6PrefixRanges)[j]).End, 10)
					((*(*nfProfilesStruct[i].BsfInfo).Ipv6PrefixRanges)[j]).End = context.Ipv6IntToIpv6String(ipv6IntEnd)
				}
			}
		}
	}
}

func buildFilter(queryParameters url.Values) bson.M {
	filter := bson.M{
		"$and": []bson.M{},
	}

	targetNfType := queryParameters["target-nf-type"][0]

	handleTargetNfType(queryParameters, filter)
	handleRequesterNfType(queryParameters, filter)
	handleServiceNames(queryParameters, filter)
	handleRequesterNfInstanceFqdn(queryParameters, filter)
	handleTargetPlmnList(queryParameters, filter)
	handleTargetNfInstanceID(queryParameters, filter)
	handleTargetNfFqdn(queryParameters, filter)
	handleSnssais(queryParameters, filter)
	handleNsiList(queryParameters, filter)
	handleDnn(queryParameters, filter, targetNfType)
	handleSmfServingArea(queryParameters, filter, targetNfType)
	handleTai(queryParameters, filter, targetNfType)
	handleAmfRegionID(queryParameters, filter, targetNfType)
	handleAmfSetID(queryParameters, filter, targetNfType)
	handleGuami(queryParameters, filter, targetNfType)
	handleSupi(queryParameters, filter, targetNfType)
	handleUeIpv4(queryParameters, filter, targetNfType)
	handleIpDomain(queryParameters, filter, targetNfType)
	handleUeIpv6Prefix(queryParameters, filter, targetNfType)
	handlePgwInd(queryParameters, filter)
	handlePgw(queryParameters, filter)
	handleGpsi(queryParameters, filter, targetNfType)
	handleExternalGroupIdentity(queryParameters, filter, targetNfType)
	handleDataSet(queryParameters, filter, targetNfType)
	handleRoutingIndicator(queryParameters, filter, targetNfType)
	handleGroupIDList(queryParameters, filter, targetNfType)
	handleDnaiList(queryParameters, filter, targetNfType)
	handleUpfIwkEpsInd(queryParameters, filter, targetNfType)
	handleChfSupportedPlmn(queryParameters, filter, targetNfType)
	handlePreferredLocality(queryParameters, filter)
	handleAccessType(queryParameters, filter)
	handleSupportedFeatures(queryParameters, filter)
	handleComplexQuery(queryParameters, filter)

	return filter
}

func handleTargetNfType(queryParameters url.Values, filter bson.M) {
	// [Query-1] target-nf-type
	targetNfType := queryParameters["target-nf-type"][0]
	if targetNfType != "" {
		targetNfTypeFilter := bson.M{
			"nfType": targetNfType,
		}
		filter["$and"] = append(filter["$and"].([]bson.M), targetNfTypeFilter)
	}
}

func handleRequesterNfType(queryParameters url.Values, filter bson.M) {
	// [Query-2] request-nf-type
	requesterNfType := queryParameters["requester-nf-type"][0]
	if requesterNfType != "" {
		requesterNfTypeFilter := bson.M{
			"$or": []bson.M{
				{"allowedNfTypes": requesterNfType},
				{"allowedNfTypes": bson.M{
					mongoOpExists: false,
				}},
			},
		}
		filter["$and"] = append(filter["$and"].([]bson.M), requesterNfTypeFilter)
	}
}

func handleServiceNames(queryParameters url.Values, filter bson.M) {
	// [Query-3] service-names
	// TODO: return exist service name
	if queryParameters[queryParamServiceNames] != nil {
		serviceNames := queryParameters[queryParamServiceNames][0]
		serviceNamesSplit := strings.Split(serviceNames, ",")
		var serviceNamesBsonArray bson.A

		for _, v := range serviceNamesSplit {
			serviceNamesBsonArray = append(serviceNamesBsonArray, v)
		}
		serviceNamesFilter := bson.M{
			"nfServices": bson.M{
				mongoOpElemMatch: bson.M{
					"serviceName": bson.M{
						// get all service in array
						"$in": serviceNamesBsonArray,
					},
					// the service need to be registered
					"nfServiceStatus": "REGISTERED",
				},
			},
		}
		filter["$and"] = append(filter["$and"].([]bson.M), serviceNamesFilter)
	}
}

func handleRequesterNfInstanceFqdn(queryParameters url.Values, filter bson.M) {
	// [Query-4] requester-nfinstance-fqdn
	if queryParameters["requester-nf-instance-fqdn"] != nil {
		requesterNfinstanceFqdn := queryParameters["requester-nf-instance-fqdn"][0]

		requesterNfinstanceFqdnFilter := bson.M{
			"$or": []bson.M{
				{
					"nfServices": bson.M{
						mongoOpElemMatch: bson.M{
							"allowedNfDomains": requesterNfinstanceFqdn,
						},
					},
				},
				{ // if not provided, allow any.
					"nfServices": bson.M{
						mongoOpElemMatch: bson.M{
							"allowedNfDomains": bson.M{
								mongoOpExists: false,
							},
						},
					},
				},
			},
		}
		filter["$and"] = append(filter["$and"].([]bson.M), requesterNfinstanceFqdnFilter)
	}
}

func handleTargetPlmnList(queryParameters url.Values, filter bson.M) {
	// [Query-5] target-plmn-list [C] = Mcc + Mnc
	// Mcc: Pattern: '^[0-9]{3}$'
	// Mnc: Pattern: '^[0-9]{2,3}$'
	if queryParameters[queryParamTargetPlmnList] != nil {
		targetPlmnListStr := queryParameters[queryParamTargetPlmnList][0]

		// The query parameter returns a JSON Array string (e.g., '[{"mcc":"208","mnc":"93"}]').
		// Changed to Slice []models.PlmnId to handle the JSON Array input correctly
		var targetPlmns []models.PlmnId

		// Use standard Unmarshal to parse the full array, replacing manual string splitting
		err := json.Unmarshal([]byte(targetPlmnListStr), &targetPlmns)

		if err != nil {
			logger.DiscoveryLog.Warnln("Unmarshal Error in targetPlmnList: ", err)
		} else {
			var targetPlmnListBsonArray bson.A

			// Iterate directly over the parsed structs
			for _, plmn := range targetPlmns {
				plmnBson := bson.M{
					"mcc": plmn.Mcc,
					"mnc": plmn.Mnc,
				}

				// Append to the list of criteria. This checks if the NF's 'plmnList' contains this specific PLMN.
				targetPlmnListBsonArray = append(targetPlmnListBsonArray, bson.M{
					"plmnList": bson.M{mongoOpElemMatch: plmnBson},
				})
			}

			// Only apply the filter if valid PLMNs were parsed
			if len(targetPlmnListBsonArray) > 0 {
				targetPlmnListFilter := bson.M{
					"$or": targetPlmnListBsonArray,
				}
				filter["$and"] = append(filter["$and"].([]bson.M), targetPlmnListFilter)
			}
		}
	}
	// [Query-6] requester-plmn-list
	// if queryParameters["requester-plmn-list"] != nil {
	// requesterPlmnPist := queryParameters["requester-plmn-list"][0]
	// TODO
	// }
}

func handleTargetNfInstanceID(queryParameters url.Values, filter bson.M) {
	// [Query-7] target-nf-instance-id
	if queryParameters["target-nf-instance-id"] != nil {
		targetNfInstanceid := queryParameters["target-nf-instance-id"][0]
		nfInstanceIdFilter := bson.M{
			"nfInstanceId": targetNfInstanceid,
		}
		filter["$and"] = append(filter["$and"].([]bson.M), nfInstanceIdFilter)
	}
}

func handleTargetNfFqdn(queryParameters url.Values, filter bson.M) {
	// [Query-8] target-nf-fqdn
	if queryParameters[queryParamTargetNfFqdn] != nil {
		targetNfFqdn := queryParameters[queryParamTargetNfFqdn][0]
		fqdnFilter := bson.M{
			"fqdn": targetNfFqdn,
		}
		filter["$and"] = append(filter["$and"].([]bson.M), fqdnFilter)
	}
}

func handleSnssais(queryParameters url.Values, filter bson.M) {
	// [Query-9] hnrf-uri
	// for Roaming

	// [Query-10] snssais
	// Pattern: '^[A-Fa-f0-9]{6}$'
	if queryParameters["snssais"] != nil {
		snssais := queryParameters["snssais"][0]
		snssaisSplit := strings.Split(snssais, ",")
		var snssaisBsonArray bson.A

		var tempSnssai string
		for i, v := range snssaisSplit {
			if i%2 == 0 {
				tempSnssai = v
			} else {
				tempSnssai += ","
				tempSnssai += v

				snssaiStruct := &models.Snssai{}
				err := json.Unmarshal([]byte(tempSnssai), snssaiStruct)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in snssaiStruct", err)
				}

				snssaiByteArray, err := bson.Marshal(snssaiStruct)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in snssaiStruct", err)
				}

				snssaiBsonM := bson.M{}
				err = bson.Unmarshal(snssaiByteArray, &snssaiBsonM)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in snssaiBsonM", err)
				}

				snssaisBsonArray = append(snssaisBsonArray, bson.M{"sNssais": bson.M{mongoOpElemMatch: snssaiBsonM}})
			}
		}

		// if not assign, serve all NF
		snssaisBsonArray = append(snssaisBsonArray, bson.M{"sNssais": bson.M{mongoOpExists: false}})

		snssaisFilter := bson.M{
			"$or": snssaisBsonArray,
		}

		filter["$and"] = append(filter["$and"].([]bson.M), snssaisFilter)
	}
}

func handleNsiList(queryParameters url.Values, filter bson.M) {
	// [Query-11] nsi-list
	if queryParameters[queryParamNsiList] != nil {
		nsiList := queryParameters[queryParamNsiList][0]
		nsiListSplit := strings.Split(nsiList, ",")
		var nsiListBsonArray bson.A
		for _, v := range nsiListSplit {
			nsiListBsonArray = append(nsiListBsonArray, v)
		}
		nsiListFilter := bson.M{
			"nsiList": bson.M{
				"$all": nsiListBsonArray,
			},
		}
		filter["$and"] = append(filter["$and"].([]bson.M), nsiListFilter)
	}
}

func handleDnn(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-12] dnn
	if queryParameters["dnn"] != nil {
		dnn := queryParameters["dnn"][0]
		var dnnFilter bson.M
		switch targetNfType {
		case "SMF":
			dnnFilter = bson.M{
				"smfInfo.sNssaiSmfInfoList": bson.M{
					mongoOpElemMatch: bson.M{
						"dnnSmfInfoList": bson.M{
							mongoOpElemMatch: bson.M{
								"dnn": dnn,
							},
						},
					},
				},
			}
		case "UPF":
			dnnFilter = bson.M{
				"upfInfo.sNssaiUpfInfoList": bson.M{
					mongoOpElemMatch: bson.M{
						"dnnUpfInfoList": bson.M{
							mongoOpElemMatch: bson.M{
								"dnn": dnn,
							},
						},
					},
				},
			}
		case "BSF":
			dnnFilter = bson.M{
				"$or": []bson.M{
					{
						"bsfInfo.dnnList": dnn,
					},
					{
						"bsfInfo.dnnList": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "PCF":
			dnnFilter = bson.M{
				"$or": []bson.M{
					{
						"pcfInfo.dnnList": dnn,
					},
					{
						"pcfInfo.dnnList": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), dnnFilter)
	}
}

func handleSmfServingArea(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-13] smf-serving-area
	if queryParameters[queryParamSmfServingArea] != nil {
		var smfServingAreaFilter bson.M
		smfServingArea := queryParameters[queryParamSmfServingArea][0]
		if targetNfType == "UPF" {
			smfServingAreaFilter = bson.M{
				"$or": []bson.M{
					{
						"upfInfo.smfServingArea": smfServingArea,
					},
					{
						"upfInfo.smfServingArea": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), smfServingAreaFilter)
	}
}

func handleTai(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-14] tai
	if queryParameters["tai"] != nil {
		var taiFilter bson.M
		tai := queryParameters["tai"][0]

		taiStruct := &models.Tai{}
		err := json.Unmarshal([]byte(tai), taiStruct)
		if err != nil {
			logger.DiscoveryLog.Warnln("Unmarshal Error in taiStruct: ", err)
		}

		taiByteArray, err := bson.Marshal(taiStruct)
		if err != nil {
			logger.DiscoveryLog.Warnln(errUnmarshalTaiByteArray, err)
		}

		taiBsonM := bson.M{}
		err = bson.Unmarshal(taiByteArray, &taiBsonM)
		if err != nil {
			logger.DiscoveryLog.Warnln(errUnmarshalTaiByteArray, err)
		}
		switch targetNfType {
		case "SMF":
			taiFilter = bson.M{
				"smfInfo.taiList": bson.M{
					mongoOpElemMatch: taiBsonM,
				},
			}
		case "AMF":
			taiFilter = bson.M{
				"amfInfo.taiList": bson.M{
					mongoOpElemMatch: taiBsonM,
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), taiFilter)
	}
}

func handleAmfRegionID(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-15] amf-region-id
	if queryParameters[queryParamAmfRegionID] != nil {
		if targetNfType == "AMF" {
			amfRegionId := queryParameters[queryParamAmfRegionID][0]
			amfRegionIdFilter := bson.M{
				"amfInfo.amfRegionId": amfRegionId,
			}
			filter["$and"] = append(filter["$and"].([]bson.M), amfRegionIdFilter)
		}
	}
}

func handleAmfSetID(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-16] amf-set-id
	if queryParameters[queryParamAmfSetID] != nil {
		if targetNfType == "AMF" {
			amfSetId := queryParameters[queryParamAmfSetID][0]
			amfSetIdFilter := bson.M{
				"amfInfo.amfSetId": amfSetId,
			}
			filter["$and"] = append(filter["$and"].([]bson.M), amfSetIdFilter)
		}
	}
}

func handleGuami(queryParameters url.Values, filter bson.M, targetNfType string) {
	// Query-17: guami
	// TODO: NOTE[1]
	if queryParameters["guami"] != nil {
		if targetNfType == "AMF" {
			guami := queryParameters["guami"][0]

			guamiStruct := &models.Guami{}
			err := json.Unmarshal([]byte(guami), guamiStruct)
			if err != nil {
				logger.DiscoveryLog.Warnln("Unmarshal Error in guamiStruct: ", err)
			}

			guamiByteArray, err := bson.Marshal(guamiStruct)
			if err != nil {
				logger.DiscoveryLog.Warnln(errUnmarshalGuamiByteArray, err)
			}

			guamiBsonM := bson.M{}
			err = bson.Unmarshal(guamiByteArray, &guamiBsonM)
			if err != nil {
				logger.DiscoveryLog.Warnln(errUnmarshalGuamiByteArray, err)
			}

			guamiFilter := bson.M{
				"amfInfo.guamiList": bson.M{
					mongoOpElemMatch: guamiBsonM,
				},
			}

			filter["$and"] = append(filter["$and"].([]bson.M), guamiFilter)
		}
	}
}

func handleSupi(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-18] supi
	var supi string
	if queryParameters["supi"] != nil {
		var supiFilter bson.M
		supi = queryParameters["supi"][0]
		supi = supi[5:]
		switch targetNfType {
		case "PCF":
			supiFilter = bson.M{
				"$or": []bson.M{
					{
						"pcfInfo.supiRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi,
								},
								"end": bson.M{
									"$gte": supi,
								},
							},
						},
					},
					{
						"pcfInfo.supiRanges": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "CHF":
			supiFilter = bson.M{
				"$or": []bson.M{
					{
						"chfInfo.supiRangeList": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi,
								},
								"end": bson.M{
									"$gte": supi,
								},
							},
						},
					},
					{
						"chfInfo.supiRangeList": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "AUSF":
			supiFilter = bson.M{
				"$or": []bson.M{
					{
						"ausfInfo.supiRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi,
								},
								"end": bson.M{
									"$gte": supi,
								},
							},
						},
					},
					{
						"ausfInfo.supiRanges": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "UDM":
			supiFilter = bson.M{
				"$or": []bson.M{
					{
						fieldUdmInfoSupiRanges: bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi,
								},
								"end": bson.M{
									"$gte": supi,
								},
							},
						},
					},
					{
						fieldUdmInfoSupiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdmInfoGpsiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdmExtGrpIDRanges: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "UDR":
			supiFilter = bson.M{
				"$or": []bson.M{
					{
						fieldUdrInfoSupiRanges: bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi,
								},
								"end": bson.M{
									"$gte": supi,
								},
							},
						},
					},
					{
						fieldUdrInfoSupiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdrInfoGpsiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdrExtGroupIDRanges: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), supiFilter)
	}
}

func handleUeIpv4(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-19] ue-ipv4-address
	if queryParameters[queryParamUeIpv4Address] != nil {
		var ueIpv4AddressFilter bson.M
		if targetNfType == "BSF" {
			ueIpv4Address := queryParameters[queryParamUeIpv4Address][0]
			ueIpv4AddressNumber := context.Ipv4ToInt(ueIpv4Address)
			ueIpv4AddressFilter = bson.M{
				"$or": []bson.M{
					{
						"bsfInfo.ipv4AddressRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": strconv.Itoa(int(ueIpv4AddressNumber)),
								},
								"end": bson.M{
									"$gte": strconv.Itoa(int(ueIpv4AddressNumber)),
								},
							},
						},
					},
					{
						"bsfInfo.ipv4AddressRanges": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), ueIpv4AddressFilter)
	}
}

func handleIpDomain(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-20] ip-domain
	if queryParameters[queryParamIpDomain] != nil {
		var ipDomainFilter bson.M
		if targetNfType == "BSF" {
			ipDomain := queryParameters[queryParamIpDomain][0]
			ipDomainFilter = bson.M{
				"$or": []bson.M{
					{
						"bsfInfo.ipDomainList": ipDomain,
					},
					{
						"bsfInfo.ipDomainList": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), ipDomainFilter)
	}
}

func handleUeIpv6Prefix(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-21] ue-ipv6-prefix
	if queryParameters[queryParamUeIpv6Prefix] != nil {
		var ueIpv6PrefixFilter bson.M
		if targetNfType == "BSF" {
			ueIpv6Prefix := queryParameters[queryParamUeIpv6Prefix][0]
			ueIpv6PrefixNumber := context.Ipv6ToInt(ueIpv6Prefix)
			ueIpv6PrefixFilter = bson.M{
				"$or": []bson.M{
					{
						"bsfInfo.ipv6PrefixRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": ueIpv6PrefixNumber.String(),
								},
								"end": bson.M{
									"$gte": ueIpv6PrefixNumber.String(),
								},
							},
						},
					},
					{
						"bsfInfo.ipv6PrefixRanges": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), ueIpv6PrefixFilter)
	}
}

func handlePgwInd(queryParameters url.Values, filter bson.M) {
	// [Query-22] pgw-ind
	if queryParameters[queryParamPgwInd] != nil {
		pgwInd := queryParameters[queryParamPgwInd][0]
		if pgwInd == "true" {
			pgwIndFilter := bson.M{
				"smfInfo.pgwFqdn": bson.M{
					mongoOpExists: true,
				},
			}
			filter["$and"] = append(filter["$and"].([]bson.M), pgwIndFilter)
		}
	}
}

func handlePgw(queryParameters url.Values, filter bson.M) {
	// [Query-23] pgw
	if queryParameters["pgw"] != nil {
		pgw := queryParameters["pgw"][0]
		pgwFilter := bson.M{
			"smfInfo.pgwFqdn": pgw,
		}
		filter["$and"] = append(filter["$and"].([]bson.M), pgwFilter)
	}
}

func handleGpsi(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-24] gpsi
	if queryParameters["gpsi"] != nil {
		var gpsiFilter bson.M
		gpsi := queryParameters["gpsi"][0]
		gpsi = gpsi[7:]
		switch targetNfType {
		case "CHF":
			gpsiFilter = bson.M{
				"$or": []bson.M{
					{
						"chfInfo.gpsiRangeList": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": gpsi,
								},
								"end": bson.M{
									"$gte": gpsi,
								},
							},
						},
					},
					{
						"chfInfo.gpsiRangeList": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "UDM":
			gpsiFilter = bson.M{
				"$or": []bson.M{
					{
						fieldUdmInfoGpsiRanges: bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": gpsi,
								},
								"end": bson.M{
									"$gte": gpsi,
								},
							},
						},
					},
					{
						fieldUdmInfoSupiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdmInfoGpsiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdmExtGrpIDRanges: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "UDR":
			gpsiFilter = bson.M{
				"$or": []bson.M{
					{
						fieldUdrInfoGpsiRanges: bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": gpsi,
								},
								"end": bson.M{
									"$gte": gpsi,
								},
							},
						},
					},
					{
						fieldUdrInfoSupiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdrInfoGpsiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdrExtGroupIDRanges: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), gpsiFilter)
	}
}

func handleExternalGroupIdentity(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-25] external-group-identity
	if queryParameters[queryParamExternalGroupIdentity] != nil {
		var externalGroupIdentityFilter bson.M
		externalGroupIdentity := queryParameters[queryParamExternalGroupIdentity][0]

		encodedGroupId := context.EncodeGroupId(externalGroupIdentity)
		switch targetNfType {
		case "UDM":
			externalGroupIdentityFilter = bson.M{
				"$or": []bson.M{
					{
						fieldUdmExtGrpIDRanges: bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": encodedGroupId,
								},
								"end": bson.M{
									"$gte": encodedGroupId,
								},
							},
						},
					},
					{
						fieldUdmInfoSupiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdmInfoGpsiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdmExtGrpIDRanges: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "UDR":
			externalGroupIdentityFilter = bson.M{
				"$or": []bson.M{
					{
						fieldUdrExtGroupIDRanges: bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": encodedGroupId,
								},
								"end": bson.M{
									"$gte": encodedGroupId,
								},
							},
						},
					},
					{
						fieldUdrInfoSupiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdrInfoGpsiRanges: bson.M{
							mongoOpExists: false,
						},

						fieldUdrExtGroupIDRanges: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), externalGroupIdentityFilter)
	}
}

func handleDataSet(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-26] data-set
	if queryParameters[queryParamDataSet] != nil {
		var dataSetFilter bson.M
		dataSet := queryParameters[queryParamDataSet]
		if targetNfType == "UDR" {
			dataSetFilter = bson.M{
				"$or": []bson.M{
					{
						"udrInfo.supportedDataSets": dataSet,
					},
					{
						"udrInfo.supportedDataSets": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), dataSetFilter)
	}
}

func handleRoutingIndicator(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-27] routing-indicator
	if queryParameters[queryParamRoutingIndicator] != nil {
		var routingIndicatorFilter bson.M
		routingIndicator := queryParameters[queryParamRoutingIndicator][0]
		switch targetNfType {
		case "AUSF":
			routingIndicatorFilter = bson.M{
				"$or": []bson.M{
					{
						"ausfInfo.routingIndicators": routingIndicator,
					},
					{
						"ausfInfo.routingIndicators": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		case "UDM":
			routingIndicatorFilter = bson.M{
				"$or": []bson.M{
					{
						"udmInfo.routingIndicators": routingIndicator,
					},
					{
						"udmInfo.routingIndicators": bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), routingIndicatorFilter)
	}
}

func handleGroupIDList(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-28] group-id-list
	if queryParameters[queryParamGroupIDList] != nil {
		var groupIdListFilter bson.M

		groupIdList := queryParameters[queryParamGroupIDList][0]
		groupIdListSplit := strings.Split(groupIdList, ",")
		var groupIdListBsonArray bson.A

		for _, v := range groupIdListSplit {
			groupIdListBsonArray = append(groupIdListBsonArray, v)
		}
		switch targetNfType {
		case "UDR":
			groupIdListFilter = bson.M{
				"udrInfo.groupId": bson.M{
					"$in": groupIdListBsonArray,
				},
			}
		case "UDM":
			groupIdListFilter = bson.M{
				"udmInfo.groupId": bson.M{
					"$in": groupIdListBsonArray,
				},
			}
		case "AUSF":
			groupIdListFilter = bson.M{
				"ausfInfo.groupId": bson.M{
					"$in": groupIdListBsonArray,
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), groupIdListFilter)
	}
}

func handleDnaiList(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-29] dnai-list
	if queryParameters[queryParamDnaiList] != nil {
		var dnaiFilter bson.M
		dnaiList := queryParameters[queryParamDnaiList][0]
		dnaiListSplit := strings.Split(dnaiList, ",")
		var dnaiListBsonArray bson.A

		for _, v := range dnaiListSplit {
			dnaiListBsonArray = append(dnaiListBsonArray, v)
		}
		if targetNfType == "UPF" {
			dnaiFilter = bson.M{
				"upfInfo.sNssaiUpfInfoList": bson.M{
					mongoOpElemMatch: bson.M{
						"dnnUpfInfoList": bson.M{
							mongoOpElemMatch: bson.M{
								"dnaiList": bson.M{
									"$in": dnaiListBsonArray,
								},
							},
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), dnaiFilter)
	}
}

func handleUpfIwkEpsInd(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-30] upf-iwk-eps-ind
	if queryParameters[queryParamUpfIwkEpsInd] != nil {
		var upfIwkEpsIndFilter bson.M
		// upfIwkEpsInd := queryParameters["upf-iwk-eps-ind"][0]
		if targetNfType == "UPF" {
			upfIwkEpsIndFilter = bson.M{
				"upfInfo.iwkEpsInd": true,
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), upfIwkEpsIndFilter)
	}
}

func handleChfSupportedPlmn(queryParameters url.Values, filter bson.M, targetNfType string) {
	// [Query-31] chf-supported-plmn
	if queryParameters[queryParamChfSupportedPlmn] != nil {
		var chfSupportedPlmnFilter bson.M
		chfSupportedPlmn := queryParameters[queryParamChfSupportedPlmn][0]
		chfSupportedPlmnStruct := &models.PlmnId{}
		err := json.Unmarshal([]byte(chfSupportedPlmn), chfSupportedPlmnStruct)
		if err != nil {
			logger.DiscoveryLog.Warnln("Unmarshal Error in chfSupportedPlmnStruct: ", err)
		}

		encodedchfSupportedPlmn := chfSupportedPlmnStruct.Mcc + chfSupportedPlmnStruct.Mnc

		if targetNfType == "CHF" {
			chfSupportedPlmnFilter = bson.M{
				"$or": []bson.M{
					{
						fieldChfInfoPlmnRangeList: bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": encodedchfSupportedPlmn,
								},
								"end": bson.M{
									"$gte": encodedchfSupportedPlmn,
								},
							},
						},
					},
					{
						fieldChfInfoPlmnRangeList: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		filter["$and"] = append(filter["$and"].([]bson.M), chfSupportedPlmnFilter)
	}
}

func handlePreferredLocality(queryParameters url.Values, filter bson.M) {
	// [Query-32]  preferred-locality
	// TODO: if no match
	if queryParameters[queryParamPreferredLocality] != nil {
		preferredLocality := queryParameters[queryParamPreferredLocality][0]
		preferredLocalityFilter := bson.M{
			"locality": preferredLocality,
		}
		filter["$and"] = append(filter["$and"].([]bson.M), preferredLocalityFilter)
	}
}

func handleAccessType(queryParameters url.Values, filter bson.M) {
	// [Query-33] access-type
	if queryParameters[queryParamAccessType] != nil {
		accessType := queryParameters[queryParamAccessType][0]
		accessTypeFilter := bson.M{
			"$or": []bson.M{
				{
					"smfInfo.accessType": accessType,
				},
				{
					"smfInfo.accessType": bson.M{
						mongoOpExists: false,
					},
				},
			},
		}
		filter["$and"] = append(filter["$and"].([]bson.M), accessTypeFilter)
	}
}

func handleSupportedFeatures(queryParameters url.Values, filter bson.M) {
	// [Query-34] supported-features
	if queryParameters[queryParamSupportedFeatures] != nil {
		supportedFeatures := queryParameters[queryParamSupportedFeatures][0]
		supportedFeaturesFilter := bson.M{
			"nfServices": bson.M{
				mongoOpElemMatch: bson.M{
					"supportedFeatures": supportedFeatures,
				},
			},
		}
		filter["$and"] = append(filter["$and"].([]bson.M), supportedFeaturesFilter)
	}
}

func handleComplexQuery(queryParameters url.Values, filter bson.M) {
	// [Query-35] complexQuery
	if queryParameters["complexQuery"] != nil {
		// translate raw data to complexQuery structure
		complexQuery := queryParameters["complexQuery"][0]
		complexQueryStruct := &models.ComplexQuery{}
		err := json.Unmarshal([]byte(complexQuery), complexQueryStruct)
		if err != nil {
			logger.DiscoveryLog.Warnln("Unmarshal Error in complexQuery: ", err)
		}
		complexQueryFilter := complexQueryFilter(complexQueryStruct)
		filter["$and"] = append(filter["$and"].([]bson.M), complexQueryFilter)
	}
}

const (
	COMPLEX_QUERY_TYPE_CNF string = "CNF"
	COMPLEX_QUERY_TYPE_DNF string = "DNF"
)

type AtomElem struct {
	value    string
	negative bool
}

func complexQueryFilter(complexQueryParameter *models.ComplexQuery) bson.M {
	complexQueryType := ""
	if complexQueryParameter.CNf != nil {
		complexQueryType = COMPLEX_QUERY_TYPE_CNF
	} else {
		complexQueryType = COMPLEX_QUERY_TYPE_DNF
	}

	// build the filter
	var filter bson.M

	if complexQueryType == COMPLEX_QUERY_TYPE_CNF {
		filter = bson.M{
			"$and": []bson.M{},
		}
		for _, cnfUnit := range complexQueryParameter.CNf.CnfUnits {
			queryParameters := make(map[string]*AtomElem)
			var cnfUnitFilter bson.M
			for _, atom := range cnfUnit.CnfUnit {
				queryParameters[atom.Attr] = &AtomElem{value: atom.Value, negative: atom.Negative}
			}
			cnfUnitFilter = complexQueryFilterSubprocess(queryParameters, complexQueryType)

			filter["$and"] = append(filter["$and"].([]bson.M), cnfUnitFilter)
		}
	} else {
		filter = bson.M{
			"$or": []bson.M{},
		}
	}
	return filter
}

func complexQueryFilterSubprocess(queryParameters map[string]*AtomElem, complexQueryType string) bson.M {
	var logicalOperator string

	switch complexQueryType {
	case COMPLEX_QUERY_TYPE_CNF:
		logicalOperator = "$or"
	case COMPLEX_QUERY_TYPE_DNF:
		logicalOperator = "$and"
	}

	filter := bson.M{
		logicalOperator: []bson.M{},
	}

	targetNfType := queryParameters["target-nf-type"].value

	addTargetNfTypeFilter(queryParameters, filter, logicalOperator, targetNfType)
	addServiceNamesFilter(queryParameters, filter, logicalOperator)
	addRequesterNfInstanceFqdnFilter(queryParameters, filter, logicalOperator)
	addTargetPlmnListFilter(queryParameters, filter, logicalOperator)
	addTargetNfInstanceIDFilter(queryParameters, filter, logicalOperator)
	addTargetNfFqdnFilter(queryParameters, filter, logicalOperator)
	addSnssaisFilter(queryParameters, filter, logicalOperator)
	addNsiListFilter(queryParameters, filter, logicalOperator)
	addDnnFilter(queryParameters, filter, logicalOperator, targetNfType)
	addSmfServingAreaFilter(queryParameters, filter, logicalOperator, targetNfType)
	addTaiFilter(queryParameters, filter, logicalOperator, targetNfType)
	addAmfRegionFilter(queryParameters, filter, logicalOperator, targetNfType)
	addAmfSetIdFilter(queryParameters, filter, logicalOperator, targetNfType)
	addGuamiFilter(queryParameters, filter, logicalOperator, targetNfType)
	addSupiFilter(queryParameters, filter, logicalOperator, targetNfType)
	addIpv4Filter(queryParameters, filter, logicalOperator, targetNfType)
	addIpDomainFilter(queryParameters, filter, logicalOperator, targetNfType)
	addIpv6PrefixFilter(queryParameters, filter, logicalOperator, targetNfType)
	addPgwIndFilter(queryParameters, filter, logicalOperator)
	addPgwFilter(queryParameters, filter, logicalOperator)
	addGpsiFilter(queryParameters, filter, logicalOperator, targetNfType)
	addExternalGroupFilter(queryParameters, filter, logicalOperator, targetNfType)
	addDataSetFilter(queryParameters, filter, logicalOperator, targetNfType)
	addRoutingIndicatorFilter(queryParameters, filter, logicalOperator, targetNfType)
	addGroupIdListFilter(queryParameters, filter, logicalOperator, targetNfType)
	addDnaiFilter(queryParameters, filter, logicalOperator, targetNfType)
	addUpfIwkEpsFilter(queryParameters, filter, logicalOperator, targetNfType)
	addChfSupportedPlmnFilter(queryParameters, filter, logicalOperator, targetNfType)
	addPreferredLocalityFilter(queryParameters, filter, logicalOperator)
	addAccessTypeFilter(queryParameters, filter, logicalOperator)
	addSupportedFeaturesFilter(queryParameters, filter, logicalOperator)

	return filter
}

func addTargetNfTypeFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-1] target-nf-type
	if targetNfType != "" {
		var targetNfTypeFilter bson.M
		targetNfType = queryParameters["target-nf-type"].value
		negative := queryParameters["target-nf-type"].negative
		if negative {
			targetNfTypeFilter = bson.M{
				"nfType": bson.M{
					"$ne": targetNfType,
				},
			}
		} else if !negative {
			targetNfTypeFilter = bson.M{
				"nfType": targetNfType,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), targetNfTypeFilter)
	}
}

// [Query-2] requester-nf-type
// requesterNfType := queryParameters["requester-nf-type"].value
// TODO
func addServiceNamesFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-3] service-names
	// TODO: return exist service name
	if queryParameters[queryParamServiceNames] != nil {
		var serviceNamesFilter bson.M
		serviceNames := queryParameters[queryParamServiceNames].value
		serviceNamesSplit := strings.Split(serviceNames, ",")
		var serviceNamesBsonArray bson.A

		for _, v := range serviceNamesSplit {
			serviceNamesBsonArray = append(serviceNamesBsonArray, v)
		}

		negative := queryParameters[queryParamServiceNames].negative
		if negative {
			serviceNamesFilter = bson.M{
				"nfServices": bson.M{
					mongoOpElemMatch: bson.M{
						"serviceName": bson.M{
							// get all service in array
							"$nin": serviceNamesBsonArray,
						},
						// the service need to be registered
						"nfServiceStatus": "REGISTERED",
					},
				},
			}
		} else if !negative {
			serviceNamesFilter = bson.M{
				"nfServices": bson.M{
					mongoOpElemMatch: bson.M{
						"serviceName": bson.M{
							// get all service in array
							"$in": serviceNamesBsonArray,
						},
						// the service need to be registered
						"nfServiceStatus": "REGISTERED",
					},
				},
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), serviceNamesFilter)
	}
}

func addRequesterNfInstanceFqdnFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-4] requester-nfinstance-fqdn
	if queryParameters[queryParamRequesterNfInstanceFqdn] != nil {
		var requesterNfinstanceFqdnFilter bson.M
		requesterNfinstanceFqdn := queryParameters[queryParamRequesterNfInstanceFqdn].value

		negative := queryParameters[queryParamRequesterNfInstanceFqdn].negative
		if negative {
			requesterNfinstanceFqdnFilter = bson.M{
				"nfServices": bson.M{
					mongoOpElemMatch: bson.M{
						"allowedNfDomains": requesterNfinstanceFqdn,
					},
				},
			}
		} else if !negative {
			requesterNfinstanceFqdnFilter = bson.M{
				"nfServices": bson.M{
					mongoOpElemMatch: bson.M{
						"allowedNfDomains": bson.M{
							"$ne": requesterNfinstanceFqdn,
						},
					},
				},
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), requesterNfinstanceFqdnFilter)
	}
}

func addTargetPlmnListFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-5] target-plmn-list [C] = Mcc + Mnc
	// Mcc: Pattern: '^[0-9]{3}$'
	// Mnc: Pattern: '^[0-9]{2,3}$'
	if queryParameters[queryParamTargetPlmnList] != nil {
		targetPlmnList := queryParameters[queryParamTargetPlmnList].value
		targetPlmnListSplit := strings.Split(targetPlmnList, ",")
		var targetPlmnListBsonArray bson.A

		var temptargetPlmn string
		for i, v := range targetPlmnListSplit {
			if i%2 == 0 {
				temptargetPlmn = v
			} else {
				temptargetPlmn += ","
				temptargetPlmn += v

				targetPlmnListtruct := &models.PlmnId{}
				err := json.Unmarshal([]byte(temptargetPlmn), targetPlmnListtruct)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in targetPlmnListstruct: ", err)
				}

				targetPlmnByteArray, err := bson.Marshal(targetPlmnListtruct)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in targetPlmnByteArray: ", err)
				}

				targetPlmnBsonM := bson.M{}
				err = bson.Unmarshal(targetPlmnByteArray, &targetPlmnBsonM)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in targetPlmnBsonM: ", err)
				}

				targetPlmnListBsonArray = append(targetPlmnListBsonArray, targetPlmnBsonM)
			}
		}

		var targetPlmnListFilter bson.M
		negative := queryParameters[queryParamTargetPlmnList].negative
		if negative {
			targetPlmnListFilter = bson.M{
				"PlmnList": bson.M{
					"$nin": targetPlmnListBsonArray,
				},
			}
		} else if !negative {
			targetPlmnListFilter = bson.M{
				"PlmnList": bson.M{
					"$in": targetPlmnListBsonArray,
				},
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), targetPlmnListFilter)
	}
}

// [Query-6] requester-plmn-list
// if queryParameters["requester-plmn-list"] != nil {
// requesterPlmnPist := queryParameters["requester-plmn-list"].value
// TODO
// }
func addTargetNfInstanceIDFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-7] target-nf-instanceid
	if queryParameters[queryParamTargetNfInstanceID] != nil {
		targetNfInstanceid := queryParameters[queryParamTargetNfInstanceID].value
		var nfInstanceIdFilter bson.M

		negative := queryParameters[queryParamTargetNfInstanceID].negative
		if negative {
			nfInstanceIdFilter = bson.M{
				"nfInstanceId": bson.M{
					"$ne": targetNfInstanceid,
				},
			}
		} else if !negative {
			nfInstanceIdFilter = bson.M{
				"nfInstanceId": targetNfInstanceid,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), nfInstanceIdFilter)
	}
}

func addTargetNfFqdnFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-8] target-nf-fqdn
	if queryParameters[queryParamTargetNfFqdn] != nil {
		targetNfFqdn := queryParameters[queryParamTargetNfFqdn].value
		fqdnFilter := bson.M{
			"fqdn": targetNfFqdn,
		}
		if queryParameters[queryParamTargetNfFqdn].negative {
			fqdnFilter = bson.M{
				"$not": fqdnFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), fqdnFilter)
	}
}

// [Query-9] hnrf-uri
// for Roaming
func addSnssaisFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-10] snssais
	// Pattern: '^[A-Fa-f0-9]{6}$'
	if queryParameters["snssais"] != nil {
		snssais := queryParameters["snssais"].value
		snssaisSplit := strings.Split(snssais, ",")
		var snssaisBsonArray bson.A

		var tempSnssai string
		for i, v := range snssaisSplit {
			if i%2 == 0 {
				tempSnssai = v
			} else {
				tempSnssai += ","
				tempSnssai += v

				snssaiStruct := &models.Snssai{}
				err := json.Unmarshal([]byte(tempSnssai), snssaiStruct)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in snssaiStruct: ", err)
				}

				snssaiByteArray, err := bson.Marshal(snssaiStruct)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in snssaiByteArray: ", err)
				}

				snssaiBsonM := bson.M{}
				err = bson.Unmarshal(snssaiByteArray, &snssaiBsonM)
				if err != nil {
					logger.DiscoveryLog.Warnln("Unmarshal Error in snssaiBsonM: ", err)
				}

				snssaisBsonArray = append(snssaisBsonArray, snssaiBsonM)
			}
		}

		snssaisFilter := bson.M{
			"snssais": bson.M{
				mongoOpElemMatch: snssaisBsonArray,
			},
		}
		if queryParameters["snssais"].negative {
			snssaisFilter = bson.M{
				"$not": snssaisFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), snssaisFilter)
	}
}

func addNsiListFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-11] nsi-list
	if queryParameters[queryParamNsiList] != nil {
		nsiList := queryParameters[queryParamNsiList].value
		nsiListSplit := strings.Split(nsiList, ",")
		var nsiListBsonArray bson.A
		for _, v := range nsiListSplit {
			nsiListBsonArray = append(nsiListBsonArray, v)
		}
		nsiListFilter := bson.M{
			"nsiList": bson.M{
				"$all": nsiListBsonArray,
			},
		}
		if queryParameters[queryParamNsiList].negative {
			nsiListFilter = bson.M{
				"$not": nsiListFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), nsiListFilter)
	}
}

func addDnnFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-12] dnn
	if queryParameters["dnn"] != nil {
		dnn := queryParameters["dnn"].value
		var dnnFilter bson.M
		switch targetNfType {
		case "SMF":
			dnnFilter = bson.M{
				"smfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"sNssaiSmfInfoList": bson.M{
							mongoOpElemMatch: bson.M{
								"dnnSmfInfoList": bson.M{
									mongoOpElemMatch: bson.M{
										"dnn": dnn[0],
									},
								},
							},
						},
					},
				},
			}
		case "UPF":
			dnnFilter = bson.M{
				"upfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"sNssaiUpfInfoList": bson.M{
							mongoOpElemMatch: bson.M{
								"dnnUpfInfoList": bson.M{
									mongoOpElemMatch: bson.M{
										"dnn": dnn,
									},
								},
							},
						},
					},
				},
			}
		case "BSF":
			dnnFilter = bson.M{
				"bsfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"dnnList": dnn[0],
					},
				},
			}
		}
		if queryParameters["dnn"].negative {
			dnnFilter = bson.M{
				"$not": dnnFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), dnnFilter)
	}
}

func addSmfServingAreaFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-13] smf-serving-area
	if queryParameters[queryParamSmfServingArea] != nil {
		var smfServingAreaFilter bson.M
		smfServingArea := queryParameters[queryParamSmfServingArea].value
		if targetNfType == "UPF" {
			smfServingAreaFilter = bson.M{
				"upfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"smfServingArea": smfServingArea,
					},
				},
			}
		}
		if queryParameters[queryParamSmfServingArea].negative {
			smfServingAreaFilter = bson.M{
				"$not": smfServingAreaFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), smfServingAreaFilter)
	}
}

func addTaiFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-14] tai
	if queryParameters["tai"] != nil {
		var taiFilter bson.M
		tai := queryParameters["tai"].value
		taiSplit := strings.Split(tai, ",")
		tempTai := taiSplit[0] + "," + taiSplit[1]

		taiStruct := &models.Tai{}
		err := json.Unmarshal([]byte(tempTai), taiStruct)
		if err != nil {
			logger.DiscoveryLog.Warnln("Unmarshal Error in taiStruct: ", err)
		}

		taiByteArray, err := bson.Marshal(taiStruct)
		if err != nil {
			logger.DiscoveryLog.Warnln(errUnmarshalTaiByteArray, err)
		}

		taiBsonM := bson.M{}
		err = bson.Unmarshal(taiByteArray, &taiBsonM)
		if err != nil {
			logger.DiscoveryLog.Warnln(errUnmarshalTaiByteArray, err)
		}
		switch targetNfType {
		case "SMF":
			taiFilter = bson.M{
				"smfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"taiList": taiBsonM,
					},
				},
			}
		case "AMF":
			taiFilter = bson.M{
				"amfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"taiList": taiBsonM,
					},
				},
			}
		}
		if queryParameters["tai"].negative {
			taiFilter = bson.M{
				"$not": taiFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), taiFilter)
	}
}

func addAmfRegionFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-15] amf-region-id
	if queryParameters[queryParamAmfRegionID] != nil {
		var amfRegionIdFilter bson.M
		if targetNfType == "AMF" {
			amfRegionId := queryParameters[queryParamAmfRegionID].value
			amfRegionIdFilter = bson.M{
				"amfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"amfRegionId": amfRegionId[0],
					},
				},
			}
		}
		if queryParameters[queryParamAmfRegionID].negative {
			amfRegionIdFilter = bson.M{
				"$not": amfRegionIdFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), amfRegionIdFilter)
	}
}

func addAmfSetIdFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-16] amf-set-id
	if queryParameters[queryParamAmfSetID] != nil {
		var amfSetIdFilter bson.M
		if targetNfType == "AMF" {
			amfSetId := queryParameters[queryParamAmfSetID].value
			amfSetIdFilter = bson.M{
				"amfInfo": bson.M{
					mongoOpElemMatch: bson.M{ // TOCHECK : elemMatch
						"amfSetId": amfSetId[0],
					},
				},
			}
		}
		if queryParameters[queryParamAmfSetID].negative {
			amfSetIdFilter = bson.M{
				"$not": amfSetIdFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), amfSetIdFilter)
	}
}

func addGuamiFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// Query-17: guami
	// TODO: NOTE[1]
	if queryParameters["guami"] != nil {
		var guamiFilter bson.M
		if targetNfType == "AMF" {
			guami := queryParameters["guami"].value
			guamiSplit := strings.Split(guami, ",")
			tempguami := guamiSplit[0] + "," + guamiSplit[1]

			guamiStruct := &models.Guami{}
			err := json.Unmarshal([]byte(tempguami), guamiStruct)
			if err != nil {
				logger.DiscoveryLog.Warnln("Unmarshal Error in guamiStruct: ", err)
			}

			guamiByteArray, err := bson.Marshal(guamiStruct)
			if err != nil {
				logger.DiscoveryLog.Warnln(errUnmarshalGuamiByteArray, err)
			}

			guamiBsonM := bson.M{}
			err = bson.Unmarshal(guamiByteArray, &guamiBsonM)
			if err != nil {
				logger.DiscoveryLog.Warnln(errUnmarshalGuamiByteArray, err)
			}

			guamiFilter = bson.M{
				"amfInfo": bson.M{
					mongoOpElemMatch: bson.M{ // TOCHECK : elemMatch
						"guamiList": bson.M{
							mongoOpElemMatch: guamiBsonM,
						},
					},
				},
			}
		}
		if queryParameters["guami"].negative {
			guamiFilter = bson.M{
				"$not": guamiFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), guamiFilter)
	}
}

func addSupiFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-18] supi
	var supi string
	if queryParameters["supi"] != nil {
		var supiFilter bson.M
		supi = queryParameters["supi"].value
		switch targetNfType {
		case "PCF":
			supiFilter = bson.M{
				"pcfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"supiRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi[0],
								},
								"end": bson.M{
									"$gte": supi[0],
								},
							},
						},
					},
				},
			}
		case "CHF":
			supiFilter = bson.M{
				"chfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"supiRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi[0],
								},
								"end": bson.M{
									"$gte": supi[0],
								},
							},
						},
					},
				},
			}
		case "AUSF":
			supiFilter = bson.M{
				"ausfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"supiRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi[0],
								},
								"end": bson.M{
									"$gte": supi[0],
								},
							},
						},
					},
				},
			}
		case "UDM":
			supiFilter = bson.M{
				"udmInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"supiRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi[0],
								},
								"end": bson.M{
									"$gte": supi[0],
								},
							},
						},
					},
				},
			}
		case "UDR":
			supiFilter = bson.M{
				"udrInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"supiRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": supi[0],
								},
								"end": bson.M{
									"$gte": supi[0],
								},
							},
						},
					},
				},
			}
		}
		if queryParameters["supi"].negative {
			supiFilter = bson.M{
				"$not": supiFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), supiFilter)
	}
}

func addIpv4Filter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-19] ue-ipv4-address
	if queryParameters[queryParamUeIpv4Address] != nil {
		var ueIpv4AddressFilter bson.M
		if targetNfType == "BSF" {
			ueIpv4Address := queryParameters[queryParamUeIpv4Address].value
			ueIpv4AddressNumber := context.Ipv4ToInt(ueIpv4Address)
			ueIpv4AddressFilter = bson.M{
				"bsfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"ipv4AddressNumberRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": ueIpv4AddressNumber,
								},
								"end": bson.M{
									"$gte": ueIpv4AddressNumber,
								},
							},
						},
					},
				},
			}
		}
		if queryParameters[queryParamUeIpv4Address].negative {
			ueIpv4AddressFilter = bson.M{
				"$not": ueIpv4AddressFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), ueIpv4AddressFilter)
	}
}

func addIpDomainFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-20] ip-domain
	if queryParameters[queryParamIpDomain] != nil {
		var ipDomainFilter bson.M
		if targetNfType == "BSF" {
			ipDomain := queryParameters[queryParamIpDomain].value
			ipDomainFilter = bson.M{
				"bsfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"ipDomain": ipDomain[0],
					},
				},
			}
		}
		if queryParameters[queryParamIpDomain].negative {
			ipDomainFilter = bson.M{
				"$not": ipDomainFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), ipDomainFilter)
	}
}

func addIpv6PrefixFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-21] ue-ipv6-prefix
	if queryParameters[queryParamUeIpv6Prefix] != nil {
		var ueIpv6PrefixFilter bson.M
		if targetNfType == "BSF" {
			ueIpv6Prefix := queryParameters[queryParamUeIpv6Prefix].value
			ueIpv6PrefixNumber := context.Ipv6ToInt(ueIpv6Prefix)
			ueIpv6PrefixFilter = bson.M{
				"bsfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"ipv6PrefixRanges": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": ueIpv6PrefixNumber,
								},
								"end": bson.M{
									"$gte": ueIpv6PrefixNumber,
								},
							},
						},
					},
				},
			}
		}
		if queryParameters[queryParamUeIpv6Prefix].negative {
			ueIpv6PrefixFilter = bson.M{
				"$not": ueIpv6PrefixFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), ueIpv6PrefixFilter)
	}
}

func addPgwIndFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-22] pgw-ind
	if queryParameters[queryParamPgwInd] != nil {
		var pgwIndFilter bson.M
		pgwInd := queryParameters[queryParamPgwInd].value
		if pgwInd == "true" {
			pgwIndFilter = bson.M{
				"smfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"pgwFqdn": bson.M{
							"$ne": "",
						},
					},
				},
			}
		}
		if queryParameters[queryParamPgwInd].negative {
			pgwIndFilter = bson.M{
				"$not": pgwIndFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), pgwIndFilter)
	}
}

func addPgwFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-23] pgw
	if queryParameters["pgw"] != nil {
		pgw := queryParameters["pgw"].value
		pgwFilter := bson.M{
			"smfInfo": bson.M{
				mongoOpElemMatch: bson.M{
					"pgwFqdn": pgw[0],
				},
			},
		}
		if queryParameters["pgw"].negative {
			pgwFilter = bson.M{
				"$not": pgwFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), pgwFilter)
	}
}

func addGpsiFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-24] gpsi
	var supi string
	if queryParameters["gpsi"] != nil {
		var gpsiFilter bson.M
		gpsi := queryParameters["gpsi"].value
		switch targetNfType {
		case "CHF":
			gpsiFilter = bson.M{
				"chfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"gpsiRangeList": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": gpsi,
								},
								"end": bson.M{
									"$gte": supi,
								},
							},
						},
					},
				},
			}
		case "UDM":
			gpsiFilter = bson.M{
				"udmInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"gpsiRangeList": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": gpsi[0],
								},
								"end": bson.M{
									"$gte": supi[0],
								},
							},
						},
					},
				},
			}
		case "UDR":
			gpsiFilter = bson.M{
				"udrInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"gpsiRangeList": bson.M{
							mongoOpElemMatch: bson.M{
								"start": bson.M{
									"$lte": gpsi[0],
								},
								"end": bson.M{
									"$gte": supi[0],
								},
							},
						},
					},
				},
			}
		}
		if queryParameters["gpsi"].negative {
			gpsiFilter = bson.M{
				"$not": gpsiFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), gpsiFilter)
	}
}

func addExternalGroupFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-25] external-group-identity
	if queryParameters[queryParamExternalGroupIdentity] != nil {
		var externalGroupIdentityFilter bson.M
		externalGroupIdentity := queryParameters[queryParamExternalGroupIdentity].value
		switch targetNfType {
		case "UDM":
			externalGroupIdentityFilter = bson.M{
				"udmInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"groupId": externalGroupIdentity,
					},
				},
			}
		case "UDR":
			externalGroupIdentityFilter = bson.M{
				"udrInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"groupId": externalGroupIdentity,
					},
				},
			}
		}
		if queryParameters[queryParamExternalGroupIdentity].negative {
			externalGroupIdentityFilter = bson.M{
				"$not": externalGroupIdentityFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), externalGroupIdentityFilter)
	}
}

func addDataSetFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-26] data-set
	if queryParameters[queryParamDataSet] != nil {
		var dataSetFilter bson.M
		dataSet := queryParameters[queryParamDataSet]
		if targetNfType == "UDR" {
			dataSetFilter = bson.M{
				"udrInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"SupportedDataSets": dataSet,
					},
				},
			}
		}
		if queryParameters[queryParamDataSet].negative {
			dataSetFilter = bson.M{
				"$not": dataSetFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), dataSetFilter)
	}
}

func addRoutingIndicatorFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-27] routing-indicator
	if queryParameters[queryParamRoutingIndicator] != nil {
		var routingIndicatorFilter bson.M
		routingIndicator := queryParameters[queryParamRoutingIndicator].value
		switch targetNfType {
		case "AUSF":
			routingIndicatorFilter = bson.M{
				"ausfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"routingIndicators": routingIndicator,
					},
				},
			}
		case "UDM":
			routingIndicatorFilter = bson.M{
				"udmInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"routingIndicators": routingIndicator,
					},
				},
			}
		}
		if queryParameters[queryParamRoutingIndicator].negative {
			routingIndicatorFilter = bson.M{
				"$not": routingIndicatorFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), routingIndicatorFilter)
	}
}

func addGroupIdListFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-28] group-id-list
	if queryParameters[queryParamGroupIDList] != nil {
		var groupIdListFilter bson.M

		groupIdList := queryParameters[queryParamGroupIDList].value
		groupIdListSplit := strings.Split(groupIdList, ",")
		var groupIdListBsonArray bson.A

		for _, v := range groupIdListSplit {
			groupIdListBsonArray = append(groupIdListBsonArray, v)
		}
		switch targetNfType {
		case "UDR":
			groupIdListFilter = bson.M{
				"udrInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"groupId": bson.M{
							"$in": groupIdListBsonArray,
						},
					},
				},
			}
		case "UDM":
			groupIdListFilter = bson.M{
				"udmInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"groupId": bson.M{
							"$in": groupIdListBsonArray,
						},
					},
				},
			}
		case "AUSF":
			groupIdListFilter = bson.M{
				"ausfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"groupId": bson.M{
							"$in": groupIdListBsonArray,
						},
					},
				},
			}
		}
		if queryParameters[queryParamGroupIDList].negative {
			groupIdListFilter = bson.M{
				"$not": groupIdListFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), groupIdListFilter)
	}
}

func addDnaiFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-29] dnai-list
	if queryParameters[queryParamDnaiList] != nil {
		var dnaiFilter bson.M
		dnaiList := queryParameters[queryParamDnaiList].value
		dnaiListSplit := strings.Split(dnaiList, ",")
		var dnaiListBsonArray bson.A

		for _, v := range dnaiListSplit {
			dnaiListBsonArray = append(dnaiListBsonArray, v)
		}
		if targetNfType == "UPF" {
			dnaiFilter = bson.M{
				"upfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"sNssaiUpfInfoList": bson.M{
							mongoOpElemMatch: bson.M{
								"dnnUpfInfoList": bson.M{
									mongoOpElemMatch: bson.M{
										"dnaiList": dnaiListBsonArray,
									},
								},
							},
						},
					},
				},
			}
		}
		if queryParameters[queryParamDnaiList].negative {
			dnaiFilter = bson.M{
				"$not": dnaiFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), dnaiFilter)
	}
}

func addUpfIwkEpsFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-30] upf-iwk-eps-ind
	if queryParameters[queryParamUpfIwkEpsInd] != nil {
		var upfIwkEpsIndFilter bson.M
		// upfIwkEpsInd := queryParameters["upf-iwk-eps-ind"].value
		if targetNfType == "UPF" {
			upfIwkEpsIndFilter = bson.M{
				"upfInfo": bson.M{
					mongoOpElemMatch: bson.M{
						"iwkEpsInd": true,
					},
				},
			}
		}
		if queryParameters[queryParamUpfIwkEpsInd].negative {
			upfIwkEpsIndFilter = bson.M{
				"$not": upfIwkEpsIndFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), upfIwkEpsIndFilter)
	}
}

func addChfSupportedPlmnFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string, targetNfType string) {
	// [Query-31] chf-supported-plmn
	if queryParameters[queryParamChfSupportedPlmn] != nil {
		var chfSupportedPlmnFilter bson.M
		chfSupportedPlmn := queryParameters[queryParamChfSupportedPlmn].value
		if targetNfType == "CHF" {
			chfSupportedPlmnFilter = bson.M{
				"$or": []bson.M{
					{
						"chfInfo": bson.M{
							mongoOpElemMatch: bson.M{
								"plmnRangeList": bson.M{
									mongoOpElemMatch: bson.M{
										"start": bson.M{
											"$lte": chfSupportedPlmn,
										},
										"end": bson.M{
											"$gte": chfSupportedPlmn,
										},
									},
								},
							},
						},
					},
					{
						fieldChfInfoPlmnRangeList: bson.M{
							mongoOpExists: false,
						},
					},
				},
			}
		}
		if queryParameters[queryParamChfSupportedPlmn].negative {
			chfSupportedPlmnFilter = bson.M{
				"$not": chfSupportedPlmnFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), chfSupportedPlmnFilter)
	}
}

func addPreferredLocalityFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-32]  preferred-locality
	// TODO: if no match
	if queryParameters[queryParamPreferredLocality] != nil {
		preferredLocality := queryParameters[queryParamPreferredLocality].value
		preferredLocalityFilter := bson.M{
			"locality": preferredLocality,
		}
		if queryParameters[queryParamPreferredLocality].negative {
			preferredLocalityFilter = bson.M{
				"$not": preferredLocalityFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), preferredLocalityFilter)
	}
}

func addAccessTypeFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-33] access-type
	if queryParameters[queryParamAccessType] != nil {
		accessType := queryParameters[queryParamAccessType].value
		accessTypeFilter := bson.M{
			"smfInfo": bson.M{
				mongoOpElemMatch: bson.M{
					"accessType": accessType[0],
				},
			},
		}
		if queryParameters[queryParamAccessType].negative {
			accessTypeFilter = bson.M{
				"$not": accessTypeFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), accessTypeFilter)
	}
}

func addSupportedFeaturesFilter(queryParameters map[string]*AtomElem, filter bson.M, logicalOperator string) {
	// [Query-34] supported-features
	if queryParameters[queryParamSupportedFeatures] != nil {
		supportedFeatures := queryParameters[queryParamSupportedFeatures].value
		supportedFeaturesFilter := bson.M{
			"nfServices": bson.M{
				mongoOpElemMatch: bson.M{
					"supportedFeatures": supportedFeatures,
				},
			},
		}
		if queryParameters[queryParamSupportedFeatures].negative {
			supportedFeaturesFilter = bson.M{
				"$not": supportedFeaturesFilter,
			}
		}
		filter[logicalOperator] = append(filter[logicalOperator].([]bson.M), supportedFeaturesFilter)
	}
}

func GetRequesterAndTargetNfTypeGivenQueryParameters(queryParameters url.Values) (requesterNfType, targetNfType string) {
	requesterNfType, targetNfType = "UNKNOWN_NF", "UNKNOWN_NF"
	if queryParameters[queryParamRequesterNFType] != nil {
		requesterNfType = fmt.Sprint(queryParameters[queryParamRequesterNFType][0])
	}
	if queryParameters[queryParamTargetNFType] != nil {
		targetNfType = fmt.Sprint(queryParameters[queryParamTargetNFType][0])
	}
	return requesterNfType, targetNfType
}
