/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

var helloApp = angular.module("helloApp", []);

function registrationFormDisplay() {
    if (document.getElementById("registrationForm").style.display === "none") {
        //if no registration form, then it will create one, if it exists, it will move to the else loop.
        document.getElementById("registrationForm").style.display = "block";
    } else {
        document.getElementById("registrationForm").style.display = "none";
        //this is the div id which is redirected to the called id in the page below.
    }
}

$(document).ready(function () {
    // Add smooth scrolling to all links in navbar + footer link
    $(".navbar a, footer a[href='#myPage']").on('click', function (event) {
        // Make sure this.hash has a value before overriding default behavior
        if (this.hash !== "") {
            // Prevent default anchor click behavior
            event.preventDefault();

            // Store hash
            var hash = this.hash;

            // Using jQuery's animate() method to add smooth page scroll
            // The optional number (900) specifies the number of milliseconds it takes to scroll to the specified area
            $('html, body').animate({
                scrollTop: $(hash).offset().top
            }, 900, function () {

                // Add hash (#) to URL when done scrolling (default click behavior)
                window.location.hash = hash;
            });
        } // End if
    });
})

