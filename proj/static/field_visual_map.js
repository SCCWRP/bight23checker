require([
    "esri/config",
    "esri/Map",
    "esri/Graphic",
    "esri/views/MapView",
    "esri/layers/FeatureLayer",
    "esri/widgets/LayerList",
    "esri/widgets/Legend",
    "esri/layers/MapImageLayer",
    "esri/layers/GeoJSONLayer",
    "esri/Graphic",
    "esri/layers/GraphicsLayer"
], function(esriConfig, Map, Graphic, MapView, FeatureLayer, LayerList, Legend, GeoJSONLayer, MapImageLayer, Graphic, GraphicsLayer) {

    const script_root = sessionStorage.script_root
    
    fetch(`${script_root}/getgeojson`, {
        method: 'POST'
    }).then(
        function (response) 
        {return response.json()
    }).then(function (data) {
        
        var points = data['points']
        var polylines = data['polylines']
        var polygons = data['polygons']
        
        console.log(points)
        console.log(polylines)
        console.log(polygons)

        arcGISAPIKey = data['arcgis_api_key']
        esriConfig.apiKey = arcGISAPIKey
        
        const map = new Map({
            basemap: "arcgis-topographic" // Basemap layer service
        });
    
        const view = new MapView({
            map: map,
            center: [-118.193741, 33.770050], //California
            zoom: 16,
            container: "viewDiv"
        });
        
        const graphicsLayer = new GraphicsLayer();
        map.add(graphicsLayer);
        

        let attr = {
            Name: "Station out of bight strata", // The name of the pipeline
            Recommendation: "Check the Error Tab", // The name of the pipeline
        };

        let popUp = {
            title: "{Name}",
            content: [
              {
                type: "fields",
                fieldInfos: [
                  {
                    fieldName: "Name"
                  },
                  {
                    fieldName: "Recommendation"
                  }
                ]
              }
            ]
        }

        if (points !== "None" ) {
            for (let i = 0; i < points.length; i++){
                
                let point = points[i]
                console.log(point)
                let simpleMarkerSymbol = {
                    type: "simple-marker",
                    color: [255,0,0],  // Red
                    size: "15px",
                    outline: {
                        color: [255, 255, 255], // White
                        width: 2
                    }
                };
                
                let pointGraphic = new Graphic({
                    geometry: point,
                    symbol: simpleMarkerSymbol,
                    attributes: attr,
                    popupTemplate: popUp
                    });

                graphicsLayer.add(pointGraphic);
            }
        }

        if (polylines !== "None" ) {
            for (let i = 0; i < polylines.length; i++){
                let polyline = polylines[i]
                
                let simpleLineSymbol = {
                    type: "simple-line",
                    color: [255,0,0], // RED
                    size: "15px"
                };
                
                let polylineGraphic  = new Graphic({
                    geometry: polyline,
                    symbol: simpleLineSymbol,
                    attributes: attr,
                    popupTemplate: popUp
                });
                graphicsLayer.add(polylineGraphic);
            }
        }

        if (polygons !== "None" ) {
            let popupTemplate = {
                title: "{Name}"
            }
            let attributes = {
                Name: "Bight Strata Layer"
            }

            for (let i = 0; i < polygons.length; i++){
                let polygon = polygons[i]
                
                let simpleFillSymbol = {
                    type: "simple-fill",
                    color: [227, 139, 79, 0.8],  // Orange, opacity 80%
                    size: "15px",
                    outline: {
                        color: [255, 255, 255],
                        width: 1
                    }
                };
                
                let polygonGraphic  = new Graphic({
                    geometry: polygon,
                    symbol: simpleFillSymbol,
                    attributes: attributes,
                    popupTemplate: popupTemplate
                });
                graphicsLayer.add(polygonGraphic);
            }
        }
        

    }
    )
});