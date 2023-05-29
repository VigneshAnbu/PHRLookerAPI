using PHRLockerAPI.Models;

namespace PHRLockerAPI.ViewModel
{
    public class VMGetfieldverificationblockwiseModel
    {

        public string district_id { get; set; }

        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string hud_id { get; set; }

        public string hud_name { get; set; }

        public string block_id { get; set; }

        public string block_gid { get; set; }

        public string block_name { get; set; }

        public string total_population { get; set; }

        public string verified_population { get; set; }

        public string unverified_population { get; set; }

        public string resident_population { get; set; }

        public string migrated_population { get; set; }

        public string nontraceable { get; set; }

        public string duplicate { get; set; }

        public string death { get; set; }

        public string visitor { get; set; }

    }
}
