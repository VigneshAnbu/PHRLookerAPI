namespace PHRLockerAPI.Models
{
    public class uhcperformanceModel
    {

        public string facilitycount { get; set; }


        public string avgdrugcount { get; set; }
        public string drugcount { get; set; }
        public string avglabcount { get; set; }
        public string labcount { get; set; }
        public string avgscreeningcount { get; set; }
        public string screeningcount { get; set; }
        public string typename { get; set; }
        public string servicename { get; set; }
        public string malecount { get; set; }
        public string femalecount { get; set; }

        public string b_name { get; set; }

        public string b_id { get; set; }
        public string b_gid { get; set; }
        public string h_name { get; set; }

        public string h_id { get; set; }
        public string h_gid { get; set; }
        public string d_name { get; set; }

        public string d_id { get; set; }
        public string d_gid { get; set; }

        public List<uhcperformanceModel> druglist { get; set; }
    }
}
