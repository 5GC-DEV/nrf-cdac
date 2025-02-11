// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0

/*
 * NRF NFManagement Service
 *
 * NRF NFManagement Service
 *
 * API version: 1.0.0
 * Generated by: OpenAPI Generator (https://openapi-generator.tech)
 */

package management

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/omec-project/nrf/logger"
	"github.com/omec-project/nrf/producer"
	"github.com/omec-project/openapi"
	"github.com/omec-project/openapi/models"
	"github.com/omec-project/util/httpwrapper"
)

// DeregisterNFInstance - Deregisters a given NF Instance
func HTTPDeregisterNFInstance(c *gin.Context) {
	// parse nfInstanceId

	req := httpwrapper.NewRequest(c.Request, nil)
	logger.ManagementLog.Debugln("***HttpRequest in DeregisterNFInstance: ", req)

	req.Params["nfInstanceID"] = c.Params.ByName("nfInstanceID")
	logger.ManagementLog.Debugln("***DeregisterNFInstance: ", c.Params.ByName("nfInstanceID"))

	httpResponse := producer.HandleNFDeregisterRequest(req)
	logger.ManagementLog.Debugln("***HttpResponse in DeregisterNFInstance: ", httpResponse)

	responseBody, err := openapi.Serialize(httpResponse.Body, "application/json")
	if err != nil {
		logger.ManagementLog.Warnln("***Serialization error in DeregisterNFInstance: ", err)
		problemDetails := models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "SYSTEM_FAILURE",
			Detail: err.Error(),
		}
		c.JSON(http.StatusInternalServerError, problemDetails)
	} else {
		logger.ManagementLog.Debugln("***Serialization successful in DeregisterNFInstance, sending response***")
		c.Data(httpResponse.Status, "application/json", responseBody)
	}
}

// GetNFInstance - Read the profile of a given NF Instance
func HTTPGetNFInstance(c *gin.Context) {
	req := httpwrapper.NewRequest(c.Request, nil)
	logger.ManagementLog.Debugln("***HttpRequest in GetNFInstance: ", req)

	req.Params["nfInstanceID"] = c.Params.ByName("nfInstanceID")
	logger.ManagementLog.Debugln("***DeregisterNFInstance: ", c.Params.ByName("nfInstanceID"))

	httpResponse := producer.HandleGetNFInstanceRequest(req)
	logger.ManagementLog.Debugln("***HttpResponse in GetNFInstance: ", httpResponse)

	responseBody, err := openapi.Serialize(httpResponse.Body, "application/json")
	if err != nil {
		logger.ManagementLog.Warnln("***Serialization error in DeregisterNFInstance: ", err)
		problemDetails := models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "SYSTEM_FAILURE",
			Detail: err.Error(),
		}
		c.JSON(http.StatusInternalServerError, problemDetails)
	} else {
		logger.ManagementLog.Debugln("***Serialization successful in GetNFInstance, sending response***")
		c.Data(httpResponse.Status, "application/json", responseBody)
	}
}

// RegisterNFInstance - Register a new NF Instance
func HTTPRegisterNFInstance(c *gin.Context) {
	var nfprofile models.NfProfile

	// step 1: retrieve http request body
	requestBody, err := c.GetRawData()
	if err != nil {
		problemDetail := models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		logger.ManagementLog.Errorf("***Get Request Body error: %+v", err)
		c.JSON(http.StatusInternalServerError, problemDetail)
		return
	}
	logger.ManagementLog.Debugln("***Successfully retrieved request body***")

	// step 2: convert requestBody to openapi models
	err = openapi.Deserialize(&nfprofile, requestBody, "application/json")
	logger.ManagementLog.Infoln()
	if err != nil {
		problemDetail := "[Request Body] " + err.Error()
		rsp := models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: problemDetail,
		}
		logger.ManagementLog.Errorf("***Get Request Body error: %+v", err)
		c.JSON(http.StatusBadRequest, rsp)
		return
	}
	logger.ManagementLog.Debugln("***Successfully deserialized request body to nfprofile***")

	// step 3: encapsulate the request by httpwrapper package
	req := httpwrapper.NewRequest(c.Request, nfprofile)
	logger.ManagementLog.Debugln("***Encapsulated request with nfprofile***")

	// step 4: call producer
	httpResponse := producer.HandleNFRegisterRequest(req)
	logger.ManagementLog.Debugln("***Received response from HandleNFRegisterRequest***")

	for key, val := range httpResponse.Header {
		c.Header(key, val[0])
	}
	logger.ManagementLog.Debugln("***Set response headers***")

	responseBody, err := openapi.Serialize(httpResponse.Body, "application/json")
	if err != nil {
		logger.ManagementLog.Warnln("***Serialization error: ", err)
		problemDetails := models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "SYSTEM_FAILURE",
			Detail: err.Error(),
		}
		c.JSON(http.StatusInternalServerError, problemDetails)
	} else {
		logger.ManagementLog.Debugln("***Successfully serialized response body, sending response***")
		c.Data(httpResponse.Status, "application/json", responseBody)
	}
}

// UpdateNFInstance - Update NF Instance profile
func HTTPUpdateNFInstance(c *gin.Context) {
	// step 1: retrieve http request body
	requestBody, err := c.GetRawData()
	if err != nil {
		problemDetail := models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		logger.ManagementLog.Errorf("***Get Request Body error: %+v", err)
		c.JSON(http.StatusInternalServerError, problemDetail)
		return
	}

	req := httpwrapper.NewRequest(c.Request, nil)
	req.Params["nfInstanceID"] = c.Params.ByName("nfInstanceID")
	req.Body = requestBody

	httpResponse := producer.HandleUpdateNFInstanceRequest(req)
	logger.ManagementLog.Debugln("***HttpResponse in HTTPUpdateNFInstance: ", httpResponse)
	responseBody, err := openapi.Serialize(httpResponse.Body, "application/json")
	if err != nil {
		logger.ManagementLog.Warnln(err)
		problemDetails := models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "SYSTEM_FAILURE",
			Detail: err.Error(),
		}
		c.JSON(http.StatusInternalServerError, problemDetails)
	} else {
		logger.ManagementLog.Debugln("***Successfully serialized response body, sending response***")
		c.Data(httpResponse.Status, "application/json", responseBody)
	}
}
