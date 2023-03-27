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

    const strataRenderer = {
        type: "unique-value",  // autocasts as new UniqueValueRenderer()
        field: "stratum",
        defaultSymbol: { type: "simple-fill",color: 'rgb(0, 109, 119)' },  
        uniqueValueInfos: [
            {
                // All features with value of "North" will be blue
                value: "Estuaries",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(0, 109, 119)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Freshwater Estuary",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(131, 197, 190)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Marina",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(136, 73, 143)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Bay",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(136, 73, 143)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Port",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(62, 92, 118)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Inner Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(224, 251, 252)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Lower Slope",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(34, 51, 59)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Mid Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(152, 193, 217)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Outer Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(28, 68, 142)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Upper Slope",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(224, 242, 233)'
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Channel Islands",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: 'rgb(60, 136, 126)'
                }
            }
        ]
    }

    const script_root = sessionStorage.script_root
    
    fetch(`${script_root}/getgeojson`, {
        method: 'POST'
    }).then(
        function (response) 
        {return response.json()
    }).then(function (data) {
        
        const points = data['points']
        const polylines = data['polylines']
        const polygons = data['polygons']
        const strataLayerId = data['strata_layer_id']
        
        console.log(points)
        console.log(polylines)
        console.log(polygons)

        arcGISAPIKey = data['arcgis_api_key']
        esriConfig.apiKey = arcGISAPIKey
        
        const bightstrata = new FeatureLayer({
            // autocasts as new PortalItem()
            portalItem: {
                id: strataLayerId
            },
            outFields: ["*"],
            renderer: strataRenderer
        });

        const map = new Map({
            basemap: "arcgis-topographic", // Basemap layer service
            layers: [bightstrata]
        });
    
        const view = new MapView({
            map: map,
            center: [-118.193741, 33.770050], //California
            zoom: 10,
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

        bightstrata.load().then(() => {

            const legend = new Legend({
                view: view,
                container: document.createElement('div'),
                layerInfos: [
                    {
                        layer: bightstrata,
                        title: 'Bight Strata',
                    },
                ],
            });
            
            //document.getElementById("viewDiv").appendChild(legend.container);
            view.ui.add(legend, "bottom-left");
        })
    
        // const legendExpand = new Expand({
        //     view: view,
        //     content: legend.container,
        //     group: "bottom-left",
        //     expanded: true,
        //   });
          

        
        
    })
      
});