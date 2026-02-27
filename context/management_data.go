// SPDX-FileCopyrightText: 2025 Intel Corporation
// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0

package context

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"

	"github.com/mitchellh/mapstructure"
	"github.com/omec-project/nrf/dbadapter"
	"github.com/omec-project/nrf/factory"
	"github.com/omec-project/nrf/logger"
	"github.com/omec-project/openapi"
	"github.com/omec-project/openapi/models"
	"go.mongodb.org/mongo-driver/bson"
)

const NRF_NFINST_RES_URI_PREFIX = factory.NRF_NFM_RES_URI_PREFIX + "/nf-instances/"

// Generates a random int between 0 and 99
func GenerateRandomNumber() (int, error) {
	maximum := big.NewInt(100)
	randomNumber, err := rand.Int(rand.Reader, maximum)
	if err != nil {
		return 0, err
	}
	return int(randomNumber.Int64()), nil
}

func NnrfNFManagementDataModel(nf *models.NfProfile, nfprofile models.NfProfile) error {
	if nfprofile.NfInstanceId != "" {
		nf.NfInstanceId = nfprofile.NfInstanceId
	} else {
		return fmt.Errorf("NfInstanceId field is required")
	}

	if nfprofile.NfType != "" {
		nf.NfType = nfprofile.NfType
	} else {
		return fmt.Errorf("NfType field is required")
	}

	if nfprofile.NfStatus != "" {
		nf.NfStatus = nfprofile.NfStatus
	} else {
		return fmt.Errorf("NfStatus field is required")
	}

	if nfprofile.PlmnList == nil && !factory.MinConfigAvailable && factory.ManagedByConfigPod {
		// logically NF should send PLMN else we need to wait for min config
		return fmt.Errorf("PlmnList absent. Local default config not available. NFType - %v", nfprofile.NfType)
	}
	// TODO : add plmn validation ??

	nnrfNFManagementCondition(nf, nfprofile)
	nnrfNFManagementOption(nf, nfprofile)

	return nil
}

func SetsubscriptionId() string {
	x, err := GenerateRandomNumber()
	if err != nil {
		logger.ManagementLog.Error(err)
	}
	return strconv.Itoa(x)
}

func nnrfNFManagementCondition(nf *models.NfProfile, nfprofile models.NfProfile) {
	// HeartBeatTimer
	if !factory.NrfConfig.Configuration.NfProfileExpiryEnable {
		// setting 1day keepAliveTimer value
		factory.NrfConfig.Configuration.NfKeepAliveTime = 24 * 60 * 60
	} else if factory.NrfConfig.Configuration.NfKeepAliveTime == 0 {
		logger.ManagementLog.Infoln("NfProfileExpiryEnable: true but keepAliveTime: 0, setting default keepAliveTimer: 60 sec")
		factory.NrfConfig.Configuration.NfKeepAliveTime = 60
	}
	nf.HeartBeatTimer = factory.NrfConfig.Configuration.NfKeepAliveTime
	logger.ManagementLog.Infof("HearBeat Timer value: %v sec", nf.HeartBeatTimer)

	// PlmnList
	if nfprofile.PlmnList != nil {
		a := make([]models.PlmnId, len(*nfprofile.PlmnList))
		copy(a, *nfprofile.PlmnList)
		nf.PlmnList = &a
	} else {
		nf.PlmnList = &[]models.PlmnId{
			factory.NrfConfig.Configuration.DefaultPlmnId,
		}
	}
	// fqdn
	if nfprofile.Fqdn != "" {
		nf.Fqdn = nfprofile.Fqdn
	}
	// interPlmnFqdn
	if nfprofile.InterPlmnFqdn != "" {
		nf.InterPlmnFqdn = nfprofile.InterPlmnFqdn
	}
	// ipv4Addresses
	if nfprofile.Ipv4Addresses != nil {
		// fmt.Println("NsiList")
		a := make([]string, len(nfprofile.Ipv4Addresses))
		copy(a, nfprofile.Ipv4Addresses)
		nf.Ipv4Addresses = a
	}
	// ipv6Addresses
	if nfprofile.Ipv6Addresses != nil {
		// fmt.Println("NsiList")
		a := make([]string, len(nfprofile.Ipv6Addresses))
		copy(a, nfprofile.Ipv6Addresses)
		nf.Ipv6Addresses = a
	}
}

func nnrfNFManagementOption(nf *models.NfProfile, src models.NfProfile) {
	copyBasicSlices(nf, src)
	copyNumericFields(nf, src)
	copySimpleFields(nf, src)

	copyUdrInfo(nf, src)
	copyUdmInfo(nf, src)
	copyAusfInfo(nf, src)
	copyAmfInfo(nf, src)
	copySmfInfo(nf, src)
	copyUpfInfo(nf, src)
	copyPcfInfo(nf, src)
	copyBsfInfo(nf, src)
	copyChfInfo(nf, src)
	copyRemainingFields(nf, src)
}

func copySlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}

func copyPtrSlice[T any](src *[]T) *[]T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(*src))
	copy(dst, *src)
	return &dst
}

func copyBasicSlices(nf *models.NfProfile, src models.NfProfile) {
	nf.SNssais = copyPtrSlice(src.SNssais)
	nf.NsiList = copySlice(src.NsiList)
	nf.AllowedPlmns = copyPtrSlice(src.AllowedPlmns)
	nf.AllowedNfTypes = copySlice(src.AllowedNfTypes)
	nf.AllowedNfDomains = copySlice(src.AllowedNfDomains)
	nf.AllowedNssais = copyPtrSlice(src.AllowedNssais)
	nf.NfServices = copyPtrSlice(src.NfServices)
}

func copyNumericFields(nf *models.NfProfile, src models.NfProfile) {
	if src.Priority > 0 && src.Priority <= 65535 {
		nf.Priority = src.Priority
	}
	if src.Capacity > 0 && src.Capacity <= 65535 {
		nf.Capacity = src.Capacity
	}
	if src.Load > 0 && src.Load <= 100 {
		nf.Load = src.Load
	}
}

func copySimpleFields(nf *models.NfProfile, src models.NfProfile) {
	if src.Locality != "" {
		nf.Locality = src.Locality
	}
}

func copyUdrInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.UdrInfo == nil {
		return
	}

	a := *src.UdrInfo
	nf.UdrInfo = &a
}

func copyUdmInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.UdmInfo == nil {
		return
	}
	a := *src.UdmInfo
	nf.UdmInfo = &a
}

func copyAusfInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.AusfInfo == nil {
		return
	}
	a := *src.AusfInfo
	nf.AusfInfo = &a
}

func copyAmfInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.AmfInfo == nil {
		return
	}
	a := *src.AmfInfo
	nf.AmfInfo = &a
}

func copySmfInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.SmfInfo == nil {
		return
	}
	a := *src.SmfInfo
	nf.SmfInfo = &a
}

func copyUpfInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.UpfInfo == nil {
		return
	}
	a := *src.UpfInfo
	nf.UpfInfo = &a
}

func copyPcfInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.PcfInfo == nil {
		return
	}
	a := *src.PcfInfo
	nf.PcfInfo = &a
}

func copyBsfInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.BsfInfo == nil {
		return
	}

	a := *src.BsfInfo

	if src.BsfInfo.Ipv4AddressRanges != nil {
		b := make([]models.Ipv4AddressRange, len(*src.BsfInfo.Ipv4AddressRanges))
		for i, v := range *src.BsfInfo.Ipv4AddressRanges {
			b[i].Start = strconv.Itoa(int(Ipv4ToInt(v.Start)))
			b[i].End = strconv.Itoa(int(Ipv4ToInt(v.End)))
		}
		a.Ipv4AddressRanges = &b
	}

	if src.BsfInfo.Ipv6PrefixRanges != nil {
		b := make([]models.Ipv6PrefixRange, len(*src.BsfInfo.Ipv6PrefixRanges))
		for i, v := range *src.BsfInfo.Ipv6PrefixRanges {
			b[i].Start = Ipv6ToInt(v.Start).String()
			b[i].End = Ipv6ToInt(v.End).String()
		}
		a.Ipv6PrefixRanges = &b
	}

	nf.BsfInfo = &a
}

func copyChfInfo(nf *models.NfProfile, src models.NfProfile) {
	if src.ChfInfo == nil {
		return
	}
	a := *src.ChfInfo
	nf.ChfInfo = &a
}

func copyRemainingFields(nf *models.NfProfile, src models.NfProfile) {
	nf.NrfInfo = src.NrfInfo
	nf.RecoveryTime = src.RecoveryTime
	nf.NfServicePersistence = src.NfServicePersistence
}

func GetNfInstanceURI(nfInstID string) string {
	return factory.NrfConfig.GetSbiUri() + NRF_NFINST_RES_URI_PREFIX + nfInstID
}

func SetLocationHeader(nfprofile models.NfProfile) string {
	var modifyUL UriList
	var locationHeader []string

	// set nfprofile location
	locationHeader = append(locationHeader, GetNfInstanceURI(nfprofile.NfInstanceId))

	collName := "urilist"
	nfType := nfprofile.NfType
	filter := bson.M{"nfType": nfType}

	ul, _ := dbadapter.DBClient.RestfulAPIGetOne(collName, filter)

	var originalUL UriList
	err := mapstructure.Decode(ul, &originalUL)
	if err != nil {
		panic(err)
	}

	// obtain location header = NF URI
	nnrfUriList(&originalUL, &modifyUL, locationHeader)
	modifyUL.NfType = nfprofile.NfType

	tmp, err := json.Marshal(modifyUL)
	if err != nil {
		logger.ManagementLog.Error(err)
	}
	putData := bson.M{}
	err = json.Unmarshal(tmp, &putData)
	if err != nil {
		logger.ManagementLog.Error(err)
	}

	if ok, _ := dbadapter.DBClient.RestfulAPIPutOne(collName, filter, putData); ok {
		logger.ManagementLog.Info("urilist update")
	} else {
		logger.ManagementLog.Info("urilist create")
	}

	return locationHeader[0]
}

func setUriListByFilter(filter bson.M, uriList *[]string) {
	filterNfTypeResultsRaw, _ := dbadapter.DBClient.RestfulAPIGetMany("Subscriptions", filter)
	var filterNfTypeResults []models.NrfSubscriptionData
	err := openapi.Convert(filterNfTypeResultsRaw, &filterNfTypeResults)
	if err != nil {
		logger.ManagementLog.Error(err)
	}

	for _, subscr := range filterNfTypeResults {
		*uriList = append(*uriList, subscr.NfStatusNotificationUri)
	}
}

func nnrfUriList(originalUL *UriList, UL *UriList, location []string) {
	var b *Links
	var flag bool
	var c []models.Link
	flag = true
	b = new(Links)
	size := len(location) + len(originalUL.Link.Item)

	// check duplicate
	for _, item := range originalUL.Link.Item {
		if item.Href == location[0] {
			flag = false
			break
		}
	}

	if flag {
		c = make([]models.Link, size)
		copy(c, originalUL.Link.Item)
		for i, loc := range location {
			c[len(originalUL.Link.Item)+i].Href = loc
		}
	} else {
		c = make([]models.Link, size-1)
		copy(c, originalUL.Link.Item)
	}

	b.Item = c
	UL.Link = *b
}

func GetNofificationUri(nfProfile models.NfProfile) []string {
	var uriList []string

	setUriListByFilter(buildNfTypeCond(nfProfile), &uriList)
	setUriListByFilter(buildNfInstanceIDCond(nfProfile), &uriList)
	setUriListByFilter(buildServiceNameCond(nfProfile), &uriList)
	setUriListByFilter(buildAmfCond(nfProfile), &uriList)
	setUriListByFilter(buildGuamiCond(nfProfile), &uriList)
	setUriListByFilter(buildNetworkSliceCond(nfProfile), &uriList)
	setUriListByFilter(buildNfGroupCond(nfProfile), &uriList)
	return uriList
}

func buildNfTypeCond(nfProfile models.NfProfile) bson.M {
	return bson.M{
		"subscrCond": bson.M{
			"nfType": nfProfile.NfType,
		},
	}
}

func buildNfInstanceIDCond(nfProfile models.NfProfile) bson.M {
	return bson.M{
		"subscrCond": bson.M{
			"nfInstanceId": nfProfile.NfInstanceId,
		},
	}
}

func buildServiceNameCond(nfProfile models.NfProfile) bson.M {
	if nfProfile.NfServices == nil {
		return nil
	}

	var serviceNames bson.A
	for _, s := range *nfProfile.NfServices {
		serviceNames = append(serviceNames, string(s.ServiceName))
	}

	return bson.M{
		"subscrCond.serviceName": bson.M{
			"$in": serviceNames,
		},
	}
}

func buildAmfCond(nfProfile models.NfProfile) bson.M {
	if nfProfile.AmfInfo == nil {
		return nil
	}

	return bson.M{
		"subscrCond": bson.M{
			"amfSetId":    nfProfile.AmfInfo.AmfSetId,
			"amfRegionId": nfProfile.AmfInfo.AmfRegionId,
		},
	}
}

func buildGuamiCond(nfProfile models.NfProfile) bson.M {
	if nfProfile.AmfInfo == nil || nfProfile.AmfInfo.GuamiList == nil {
		return nil
	}

	var orArray bson.A

	for _, guami := range *nfProfile.AmfInfo.GuamiList {
		guamiMarshal := marshalToBson(guami)
		orArray = append(orArray,
			bson.M{"subscrCond": bson.M{"$elemMatch": guamiMarshal}},
		)
	}

	return bson.M{"$or": orArray}
}

func buildNetworkSliceCond(nfProfile models.NfProfile) bson.M {
	if nfProfile.SNssais == nil {
		return nil
	}

	var snssaisArray bson.A
	for _, snssai := range *nfProfile.SNssais {
		snssaisArray = append(snssaisArray,
			bson.M{"subscrCond": bson.M{"$elemMatch": marshalToBson(snssai)}},
		)
	}

	if nfProfile.NsiList == nil {
		return bson.M{
			"$and": bson.A{
				bson.M{"$or": snssaisArray},
			},
		}
	}

	var nsiArray bson.A
	for _, nsi := range nfProfile.NsiList {
		nsiArray = append(nsiArray, nsi)
	}

	return bson.M{
		"$and": bson.A{
			bson.M{
				"subscrCond.nsiList": bson.M{"$in": nsiArray},
			},
			bson.M{"$or": snssaisArray},
		},
	}
}

func buildNfGroupCond(nfProfile models.NfProfile) bson.M {
	groupID := ""

	switch {
	case nfProfile.UdrInfo != nil:
		groupID = nfProfile.UdrInfo.GroupId
	case nfProfile.UdmInfo != nil:
		groupID = nfProfile.UdmInfo.GroupId
	case nfProfile.AusfInfo != nil:
		groupID = nfProfile.AusfInfo.GroupId
	default:
		return nil
	}

	return bson.M{
		"subscrCond": bson.M{
			"nfType":    nfProfile.NfType,
			"nfGroupId": groupID,
		},
	}
}

func marshalToBson(v interface{}) bson.M {
	tmp, err := json.Marshal(v)
	if err != nil {
		logger.ManagementLog.Error(err)
	}

	result := bson.M{}
	if err = json.Unmarshal(tmp, &result); err != nil {
		logger.ManagementLog.Error(err)
	}

	return result
}

func NnrfUriListLimit(originalUL *UriList, limit int) {
	// response limit

	if limit < len(originalUL.Link.Item) {
		b := new(Links)
		c := make([]models.Link, limit)
		copy(c, originalUL.Link.Item[:limit])
		b.Item = c
		originalUL.Link = *b
	}
}
