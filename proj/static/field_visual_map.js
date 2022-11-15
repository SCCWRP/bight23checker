require([
    "esri/Map",
    "esri/Graphic",
    "esri/views/MapView",
    "esri/layers/FeatureLayer",
    "esri/widgets/LayerList",
    "esri/widgets/Legend",
    "esri/core/watchUtils",
    "esri/layers/MapImageLayer"
], function(Map, Graphic, MapView, FeatureLayer, LayerList, Legend, watchUtils, MapImageLayer) {
    
    // console.log("badTrawlLayerID")
    // var badTrawlLayerID = {{ bad_grab_layer_id|safe }};
    // console.log(badTrawlLayerID)

    const badStationLayer = new FeatureLayer({
        // autocasts as new PortalItem()
        portalItem: {
            id: "8854eb9280c5467784e74e13ad821fc2" e
        },
        outFields: ["*"]
    });
    

    
    const compMap = new Map({
        //basemap: "gray-vector",
        basemap: "topo",
        layers: [badStationLayer]
    });

    const view = new MapView({
        container: "viewDiv",
        map: compMap
    });

    badStationLayer.when(() => {
        return badStationLayer.queryExtent();
    }).then((response) => {
        view.goTo(response.extent);
    });

});