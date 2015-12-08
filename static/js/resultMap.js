var map;
var labels = '123456789';
var labelIndex = 0;

function initialize(locations) {
var mapOptions = {
            zoom: 8,
            center: new google.maps.LatLng(-33.890542, 151.274856),
            mapTypeId: google.maps.MapTypeId.ROADMAP,
            styles: [{"featureType":"administrative","elementType":"labels.text.fill","stylers":[{"color":"#444444"}]},{"featureType":"landscape","elementType":"all","stylers":[{"color":"#f2f2f2"}]},{"featureType":"poi","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"road","elementType":"all","stylers":[{"saturation":-100},{"lightness":45}]},{"featureType":"road.highway","elementType":"all","stylers":[{"visibility":"simplified"}]},{"featureType":"road.arterial","elementType":"labels.icon","stylers":[{"visibility":"off"}]},{"featureType":"transit","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"water","elementType":"all","stylers":[{"color":"#46bcec"},{"visibility":"on"}]}]

            };

    //var locations = {{ data|safe }};

    map = new google.maps.Map(document.getElementById('map'),mapOptions);
        //Add 1st marker

    var i, LatLng_n, marker_n
    for (i = 0; i < locations.length; i++) {
      var Latlng_n = new google.maps.LatLng(locations[i][1], locations[i][2]);
      var marker_n = new google.maps.Marker({
      position: Latlng_n,
      label: labels[labelIndex++ % labels.length],
      title:String(i)});

        marker_n.setMap(map);
      }


    //Add 2nd marke
    var Latlng_10 = new google.maps.LatLng(-34.950198, 151.259302);
    var marker_10 = new google.maps.Marker({
        position: Latlng_10,
        label: labels[labelIndex++ % labels.length],
        title:"10"});
        marker_10.setMap(map);


    }
initialize({{ data|safe }});
