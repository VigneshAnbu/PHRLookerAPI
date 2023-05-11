using PHRLockerAPI.Models;

namespace PHRLockerAPI.ViewModel
{
    public class VMPbsModel
    {
        public List<VMPbsModel> DistrictWise { get; set; }

        public string district_id { get; set; }
        

        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string TotalCount { get; set; }
        public string screeningCount { get; set; }
        public string uniqueCount { get; set; }
        public string referredscreening { get; set; }

        public string hud_id { get; set; }
        public string hud_gid { get; set; }
        public string hud_name { get; set; }
        public string block_name { get; set; }
        public string block_id { get; set; }
        public string block_gid { get; set; }


        public string village_id { get; set; }
        public string village_gid { get; set; }
        public string village_name { get; set; }
    }
}
