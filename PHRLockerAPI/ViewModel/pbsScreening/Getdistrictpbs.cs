using PHRLockerAPI.Models;

namespace PHRLockerAPI.ViewModel
{
    public class Getdistrictpbs
    {
        public List<Getdistrictpbs> DistrictWise { get; set; }

        public string district_id { get; set; }
        

        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string TotalCount { get; set; }
        public string screeningCount { get; set; }
        public string uniqueCount { get; set; }
        public string referredscreening { get; set; }

    }
}
