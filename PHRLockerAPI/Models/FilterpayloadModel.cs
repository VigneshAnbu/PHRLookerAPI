using Microsoft.AspNetCore.Mvc;
using System.Xml.Linq;

namespace PHRLockerAPI.Models
{
    public class FilterpayloadModel
    {
        [FromQuery(Name = "district_id")]
        public string? district_id { get; set; } = "";

        [FromQuery(Name = "hud_id")]
        public string ? hud_id { get; set; } = "";

        [FromQuery(Name = "block_id")]
        public string ? block_id { get; set; } = "";

        [FromQuery(Name = "facility_id")]
        public string ? facility_id { get; set; } = "";

        [FromQuery(Name = "indistrict_id")]
        public string ? indistrict_id { get; set; } = "";


        [FromQuery(Name = "inhud_id")]
        public string ? inhud_id { get; set; } = "";

        [FromQuery(Name = "inblock_id")]
        public string ? inblock_id { get; set; } = "";

        [FromQuery(Name = "infacility_id")]
        public string ? infacility_id { get; set; } = "";

        [FromQuery(Name = "village_id")]
        public string ? village_id { get; set; } = "";

        [FromQuery(Name = "directorate_id")]
        public string ? directorate_id { get; set; } = "";

        [FromQuery(Name = "role")]
        public string ? role { get; set; } = "";
    }

}
