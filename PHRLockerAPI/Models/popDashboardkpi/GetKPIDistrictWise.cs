using PHRLockerAPI.Models;

namespace PHRLockerAPI.ViewModel
{
    public class GetKPIDistrictWise
    {


        public Guid district_id { get; set; }

        public string district_name { get; set; }

        public string district_gid { get; set; }


        public string total_population { get; set; }

        public string verified_population { get; set; }

        public string unverified_population { get; set; }

        public string resident_population { get; set; }

        public string migrated_population { get; set; }

        public string nontraceable { get; set; }

        public string duplicate { get; set; }

        public string death { get; set; }

        public string consent { get; set; }

        public string allocated_streets { get; set; }

        public string visitor { get; set; }

    }
}
