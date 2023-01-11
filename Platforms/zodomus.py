import requests
import json

from requests.auth import HTTPBasicAuth

secrets = json.load(open('../config_secrets.json'))
resources = json.load(open('../Databases/resources_help.json'))

url = "https://api.zodomus.com/rooms-activation"

r = requests.post(url=url,
                  auth=HTTPBasicAuth(secrets["zodomus"]["api_user"], secrets["zodomus"]["api_password"]),
                  params={
                      "channelId": 1,
                      "propertyId": "7332800",
                      "rooms": [
                          {
                              "roomId": "733280001",
                              "roomName": "One-Bedroom Apartment",
                              "quantity": 1,
                              "status": 1,
                              "rates": [
                                  {"22052463"}
                              ]
                          }
                      ]
                  })

print(json.loads(r.content))

resp = {
    "status": {
        "returnCode": 200,
        "returnMessage": "Airbnb Host request accepted",
        "channelLogId": "",
        "channelOtherMessages": "",
        "timestamp": "2023-01-10 11:41:38"
    },
    "token": 484785181,
    "client_id": "9634cBTrdfsgspmAgTrTap9uMhK43tR5"
}

# {"status":{"returnCode":"400","returnMessage":{"Property status":"Evaluation OTA","Channel status":"OK","Product status":"Error: Some room\/rates are not maped with the channel room\/rates. Use \/rooms-activation to map room\/rates","Room status":"Error: Some rooms are not maped with the channel rooms. Use \/rooms-activation to map room\/rates"},"channelLogId":"","channelOtherMessages":"","timestamp":"2023-01-11 12:11:53"},"mappedProducts":[{"roomId":"733280001","rateId":"7332800991","myRoomId":"","myRateId":""},{"roomId":"733280001","rateId":"7332800992","myRoomId":"","myRateId":""},{"roomId":"733280001","rateId":"7332800993","myRoomId":"","myRateId":""},{"roomId":"733280002","rateId":"7332800991","myRoomId":"","myRateId":""},{"roomId":"733280002","rateId":"7332800992","myRoomId":"","myRateId":""},{"roomId":"733280002","rateId":"7332800993","myRoomId":"","myRateId":""},{"roomId":"733280003","rateId":"7332800991","myRoomId":"","myRateId":""},{"roomId":"733280003","rateId":"7332800992","myRoomId":"","myRateId":""},{"roomId":"733280003","rateId":"7332800993","myRoomId":"","myRateId":""}],"mappedRooms":[{"roomId":"733280001","myRoomId":""},{"roomId":"733280002","myRoomId":""},{"roomId":"733280003","myRoomId":""}]}
# {"status":{"returnCode":200,"returnMessage":"Ok","channelLogId":"","channelOtherMessages":"","timestamp":"2023-01-10 11:48:14"},"response":{"listings":[{"id":"12345000","name":"Apartamento em Telheiras , Lisboa","propertyTypeGroup":"apartments","propertyTypeCategory":"condominium","roomTypeCategory":"entire_home","bedrooms":3,"bathrooms":2,"beds":2,"amenityCategories":["cooking_basics","elevator","essentials","fire_extinguisher","first_aid_kit","free_parking"],"checkInOption":null,"listingApprovalStatus":{"statusCategory":"approved","notes":null},"hasAvailability":true,"permitOrTaxId":"Exempt","apt":"8 Dto","street":"Rua Abel Salazar 36","city":"Lisboa","state":"Lisboa","zipcode":"1600-818","countryCode":"PT","lat":38.768127,"lng":-9.167902,"userDefinedLocation":false,"directions":"","personCapacity":4,"listingCurrency":"EUR","listingPrice":0,"synchronizationCategory":"sync_all","bathroomShared":null,"bathroomSharedWithCategory":null,"commonSpacesShared":null,"commonSpacesSharedWithCategory":null,"totalInventoryCount":null,"propertyExternalId":null,"listingNickname":null,"tier":"marketplace","displayExactLocationToGuest":false,"houseManual":null,"wifiNetwork":null,"wifiPassword":null,"amenities":{"TV":{"instruction":"","isPresent":true,"metadata":null},"SMOKEDETECTOR":{"instruction":"","isPresent":true,"metadata":null}},"ratePlanEnabled":null,"listingViews":null,"idStr":"12345000"}],"paging":{"totalCount":3,"limit":1,"prevOffset":null,"nextOffset":1,"nextCursor":"FgIWjKLxkgwA"}}}
