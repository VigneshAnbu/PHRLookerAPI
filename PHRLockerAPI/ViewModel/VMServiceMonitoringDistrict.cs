namespace PHRLockerAPI.ViewModel
{
    public class VMServiceMonitoringDistrict
    {
        public Guid district_id { get; set; }
        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string totalscreening { get; set; }

        public string totallabtest { get; set; }

        public string labtest30 { get; set; }

        public string screeningperuser { get; set; }

        public string streetswithundelivered { get; set; }

        public string streetswithundelivered90 { get; set; }

        public string streetswithservicesdelivered { get; set; }

        public string streetswithnoscreenings90 { get; set; }


    }
}
