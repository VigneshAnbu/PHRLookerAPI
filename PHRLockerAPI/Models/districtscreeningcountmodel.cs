namespace PHRLockerAPI.Models
{
    public class districtscreeningcountmodel
    {
        public string district_name { get; set; }

        public string district_id { get; set; }
        public string district_gid { get; set; }

        public string TotalCount { get; set; }

        public string screeningCount { get; set; }

        public string uniqueCount { get; set; }

        public string onescreening { get; set; }

        public string below18 { get; set; }


        public string age_18_30 { get; set; }

        public string above30 { get; set; }

        public string multiscreening { get; set; }


        public string suspected { get; set; }
        public string suspectedconfirmed { get; set; }
        public string drugissued { get; set; }
        public string scrpercentage { get; set; }

        public List<districtscreeningcountmodel> DistrictWise { get; set; }

        public string dmconfirmed { get; set; }
        public string tc_count { get; set; }
        public string ref_count { get; set; }
        public string rc_count { get; set; }

    }
}
