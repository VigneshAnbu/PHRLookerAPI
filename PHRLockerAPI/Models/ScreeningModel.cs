using Microsoft.AspNetCore.Authorization;
using System.Net.Sockets;
using System.Reflection.Metadata.Ecma335;

namespace PHRLockerAPI.Models
{
    public class ScreeningModel
    {

        public string hsc_name { get; set; }

        public string diagnosis_name { get; set; }
        public string screening_id { get; set; }

        public string drugs { get; set; }
        public string screeningdate { get; set; }
        public string bmi { get; set; }

        public string breathe_difficulty { get; set; }

        public string chestpain { get; set; }

        public string dia_bp { get; set; }

        public string dim_vision { get; set; }

        public string dizziness { get; set; }

        public string dm_risk_score { get; set; }

        public string dm_screening { get; set; }

        public string fatigue { get; set; }

        public string freq_urine { get; set; }

        public string height { get; set; }

        public string hip_circumference { get; set; }

        public string ht_screening { get; set; }

        public string nota_diabetes { get; set; }

        public string nota_htn { get; set; }

        public string palpitation { get; set; }

        public string pulse_rate { get; set; }

        public string rbs { get; set; }

        public string rbs_date { get; set; }

        public string rr { get; set; }

        public string spo2 { get; set; }

        public string sys_bp { get; set; }

        public string tempr { get; set; }

        public string thirsty { get; set; }

        public string waist_circumference { get; set; }

        public string waist_hip_ratio { get; set; }

        public string weight { get; set; }



    }
}   
        