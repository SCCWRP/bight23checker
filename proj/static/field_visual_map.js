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
        
        arcGISAPIKey = data['arcgis_api_key']
        esriConfig.apiKey = arcGISAPIKey
        
        const map = new Map({
            basemap: "arcgis-topographic" // Basemap layer service
            });
    
        const view = new MapView({
            map: map,
            center: [-119.417931, 36.778259], //California
            zoom: 2,
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

        for (let i = 0; i < points.length; i++){
            
            let point = points[i]

            let simpleMarkerSymbol = {
                type: "simple-marker",
                color: [255,0,0],  // Red
                outline: {
                    color: [255, 255, 255], // White
                    width: 1
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
        
        graphicsLayer.when(function(){
            view.extent = graphicsLayer.fullExtent;
          });

        for (let i = 0; i < polylines.length; i++){
            let polyline = polylines[i]
            
            let simpleLineSymbol = {
                type: "simple-line",
                color: [255,0,0], // RED
                width: 2
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
    )
});