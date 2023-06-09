using Microsoft.AspNetCore.Mvc;
using System.Xml.Linq;

namespace PHRLockerAPI.Models
{
    public class loginmodel
    {
        [FromQuery(Name = "mobile")]
        public string mobile { get; set; }

        [FromQuery(Name = "password")]
        public string password { get; set; }


    }
}
