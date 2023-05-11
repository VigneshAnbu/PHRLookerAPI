namespace PHRLockerAPI.Models
{
    public class mtmkpi
    {


        public string? district_id { get; set; }

        public string? district_name { get; set; }

        public string? district_gid { get; set; }

        public string? ScreeningValues { get; set; }

        

        public double? ht { get; set; }
        public double? dt { get; set; }
        public double? htdt { get; set; }
        public double? pallative { get; set; }

        public double? physio { get; set; }
        public double? capd { get; set; }

        //public string hud_id { get; set; }
        //public string hud_gid { get; set; }
        //public string hud_name { get; set; }
        public string? block_name { get; set; }
        public string? block_id { get; set; }
        public string? block_gid { get; set; }
        //public string mtmcount { get; set; }
        //public string drugcount { get; set; }
        public double? drugreceived { get; set; }
        public double? mtmcount { get; set; }
        public double? uniquescreening { get; set; }
        public double? totalscreening { get; set; }
        public double? mtmdrugreceived { get; set; }

    }
}
