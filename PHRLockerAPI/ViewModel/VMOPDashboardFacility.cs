namespace PHRLockerAPI.ViewModel
{
    public class VMOPDashboardFacility
    {

        public string facility_id { get; set; }

        public string facility_name { get; set; }

        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string hud_name { get; set; }

        public string hud_gid { get; set; }

        public string directorate_name { get; set; }

        public string block_name { get; set; }

        public string block_gid { get; set; }

        public string facility_type_name { get; set; }

        public string is_hwc { get; set; }

        public int TotalOP { get; set; }

        public int AdultOPMale { get; set; }
        public int AdultOPFemale { get; set; }
        public int AdultOPtransgender { get; set; }
        public int AdultTotal { get; set; }
        public int ChildrenOPMale { get; set; }
        public int ChildrenOPFemale { get; set; }
        public int ChildrenOPtransgender { get; set; }
        public int ChildrenTotal { get; set; }
        public int TotalOPMale { get; set; }
        public int TotalOPFemale { get; set; }
        public int TotalOPtransgender { get; set; }





    }
}
