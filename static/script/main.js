$(document).ready(function() {

   /* Loader Script */
    $('.loader').show();
    setTimeout(function() {
     $('.loader').hide();
    }, 1500);

   /* Navbar script */

   // Get the current URL
  var url = window.location.href;

  // Get all navbar links
  var links = document.querySelectorAll('#navbarList a');

  // Loop through the links and remove the "active" class from all links
  for (var i = 0; i < links.length; i++) {
    links[i].classList.remove('active');
  }

  // Loop through the links again and add the "active" class to the corresponding link
  for (var i = 0; i < links.length; i++) {
    var link = links[i];
    if (link.href === url) {
      link.classList.add('active');
    }
  }

   /* DataTable script */
   $(".airlines_table,.yearly_distance_table").DataTable({
     pageLength:25
   });

});
