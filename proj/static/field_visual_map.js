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
    const blueColors = ["#eff3ff","#bdd7e7","#6baed6","#3182bd","#08519c"];
    const greenColors = ["#edf8e9","#bae4b3","#74c476","#31a354","#006d2c"];
    const purpleColors = ["#f2f0f7","#cbc9e2","#9e9ac8","#756bb1","#54278f"];
    const colorForTheSpecifiedRegionOfTheUser = '#db162f';  // fire engine red
    const strataRenderer = {
        type: "unique-value",  // autocasts as new UniqueValueRenderer()
        field: "stratum",
        defaultSymbol: { type: "simple-fill",color: 'rgb(0, 109, 119)' },  
        uniqueValueInfos: [
            {
                // All features with value of "North" will be blue
                value: "Your Region",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: colorForTheSpecifiedRegionOfTheUser
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Bay",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColors[4]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Marina",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColors[3]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Port",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: purpleColors[2]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Estuaries",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColors[1]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Freshwater Estuary",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColors[0]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Inner Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColors[0]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Mid Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColors[1]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Outer Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColors[2]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Upper Slope",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColors[3]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Lower Slope",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColors[4]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Channel Islands",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: purpleColors[4]
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
        

        // let attr = {
        //     Name: "Station out of the specified region", // The name of the pipeline
        //     Recommendation: "Check the Error Tab", // The name of the pipeline
        // };

        

        if (points !== "None" ) {
            let popUp = {
                title: "{stationid}",
                content: `
                    <p><strong>Your station {stationid} was not found inside the region which was specified in the data ({region})</strong></p>
                    <p>This point corresponds to grab event number: {grabeventnumber}</p>
                    <p>The Region specified in your data was: {region}</p>
                    <p>The Stratum specified in your data was: {stratum}</p>
                `
            }
            for (let i = 0; i < points.length; i++){
                
                let point = points[i].geometry

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
                    attributes: points[i].properties,
                    popupTemplate: popUp
                    });

                graphicsLayer.add(pointGraphic);
            }
        }

        if (polylines !== "None" ) {
            let popUp = {
                title: "{stationid}",
                content: `
                    <p><strong>Your station {stationid} was not found inside the region which was specified in the data ({region})</strong></p>
                    <p>This line corresponds to trawl number: {trawlnumber}</p>
                    <p>The Region specified in your data was: {region}</p>
                    <p>The Stratum specified in your data was: {stratum}</p>
                `
            }
            for (let i = 0; i < polylines.length; i++){
                let polyline = polylines[i].geometry
                console.log('polyline')
                console.log(polyline)
                
                let simpleLineSymbol = {
                    type: "simple-line",
                    color: [255,0,0], // RED
                    size: "15px"
                };
                
                let polylineGraphic  = new Graphic({
                    geometry: polyline,
                    symbol: simpleLineSymbol,
                    attributes: polylines[i].properties,
                    popupTemplate: popUp
                });
                graphicsLayer.add(polylineGraphic);
            }
        }

        if (polygons !== "None" ) {
            let popupTemplate = {
                title: "{region}",
                content: `
                    <p>The Region specified in your data submission was: {region}</p>
                    <p>The Stratum specified in your submission was: {stratum}</p>
                    <p><strong>The Lat/Longs for your station {stationid} were not found in this region ({region})</strong></p>
                `
            }
            // let attributes = {
            //     Name: "Bight Strata Layer"
            // }

            console.log('polygons')
            console.log(polygons)
            for (let i = 0; i < polygons.length; i++){
                let polygon = polygons[i].geometry
                let attributes = polygons[i].properties
                console.log('polygon')
                console.log(polygon)
                
                let simpleFillSymbol = {
                    type: "simple-fill",
                    color: colorForTheSpecifiedRegionOfTheUser, 
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