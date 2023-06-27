namespace PHRLockerAPI.Models
{
    public class PopulationdashboardHUDModel
    {
        public Guid hud_id { get; set; }

        public string hud_name { get; set; }

        public string district_gid { get; set; }

        public string district_name { get; set; }

        public string total_population { get; set; }

        public string verified_population { get; set; }

        public string male_population { get; set; }

        public string female_population { get; set; }

        public string other_population { get; set; }

        public string aadharlinkedmembers { get; set; }

        public string citizenwithudid { get; set; }

        public string total_disability { get; set; }

        public string total_families { get; set; }


        public string cmchis { get; set; }

        public string familiesadded { get; set; }

        public string membersadded { get; set; }
    }
}
