namespace PHRLockerAPI.Models
{
    public class UserPerformanceModel
    {
        public string district_id { get; set; }
        
        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string block_name { get; set; }

        public string block_gid { get; set; }

        public string hud_name { get; set; }

        public string hud_gid { get; set; }


        public string syncedscreening24 { get; set; }

        public string syncedscreening48 { get; set; }

        public string syncedscreening30 { get; set; }

        public string syncedAverage { get; set; }

        public string individualscreenings24 { get; set; }

        public string individualscreenings30 { get; set; }

        public string individualAverage { get; set; }

        public string familyscreenings24 { get; set; }

        public string familyscreenings30 { get; set; }

        public string familyscreeningsAverage { get; set; }

        public string drugissued24 { get; set; }

        public string drugissued30 { get; set; }

        public string drugissuedAverage { get; set; }

        public string mtmcount { get; set; }

        public string drugcount { get; set; }

    }
}
