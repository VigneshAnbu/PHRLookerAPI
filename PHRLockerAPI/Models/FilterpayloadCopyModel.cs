using Microsoft.AspNetCore.Mvc;
using System.Xml.Linq;

namespace PHRLockerAPI.Models
{
    public class FilterpayloadCopyModel
    {

        
        public string district_id { get; set; } 

        
        public string hud_id { get; set; } 

        
        public string block_id { get; set; } 

        
        public string facility_id { get; set; } 

        
        public string indistrict_id { get; set; } 


        
        public string inhud_id { get; set; } 

        
        public string inblock_id { get; set; } 

        
        public string infacility_id { get; set; } 


        public string village_id { get; set; }


        public string directorate_id { get; set; } 

        
        public string role { get; set; }
    }
}
