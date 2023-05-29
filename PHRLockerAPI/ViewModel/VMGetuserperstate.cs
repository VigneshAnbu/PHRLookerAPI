using PHRLockerAPI.Models;

namespace PHRLockerAPI.ViewModel
{
    public class VMGetuserperstate
    {

        public List<GetuserperstateModel> UserList { get; set; }

        public string individualscreeningsaverage { get; set; }

        public string familyscreeningsaverage { get; set; }

        public string drugissuedaverage { get; set; }

        public string T_SyncedScreenings24 { get; set; }

        public string T_SyncedScreenings48 { get; set; }

        public string T_SyncedScreenings30 { get; set; }

        public string T_FamilyScreenings24 { get; set; }

        public string T_FamilyScreenings30 { get; set; }

        public string T_DrugIssued24 { get; set; }

        public string T_DrugIssued30 { get; set; }

        public string T_IndividualScreenings24 { get; set; }

        public string T_IndividualScreenings30 { get; set; }


    }
}
