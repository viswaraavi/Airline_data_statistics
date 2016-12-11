using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

using System.Web;

namespace DIC_Airline_Project
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        string _url;
        string _emrUrl = "http://ec2-54-152-179-188.compute-1.amazonaws.com/";
        public List<string> CityList = new List<string>()
        {
            "Minneapolis, MN",
            "Nashville, TN",
            "Portland, OR",
            "Los Angeles, CA",
            "San Jose, CA",
            "Seattle, WA",
            "Phoenix, AZ",
            "New York, NY",
            "Miami, FL",
            "Boston, MA",
            "Houston, TX",
            "San Diego, CA",
            "Atlanta, GA",
            "Orlando, FL",
            "Detroit, MI",
            "Salt Lake City, UT",
            "Honolulu, HI",
            "Washington, DC",
            "Newark, NJ",
            "Philadelphia, PA",
            "Raleigh/Durham, NC",
            "Las Vegas, NV"
        };

        public List<string> AirportList = new List<string>()
        {
            "JFK",
            "GSP",
            "FNT",
            "GST",
            "ILG",
            "MIA",
            "BOS",
            "OAK",
            "FWA",
            "LIT",
            "BOI",
            "BGR",
            "DRO"
        };

       



        public Dictionary<string, string> CarrierDict = new Dictionary<string, string>();        

        public MainWindow()
        {
            PopulateDict();
            InitializeComponent(); 
            comboBoxDest.ItemsSource = CityList;
            comboBoxOrigin.ItemsSource = CityList;
            comboBoxCarrier.ItemsSource = CarrierDict.Values;
            comboBoxDestAirport.ItemsSource = AirportList;
            comboBoxOriginAirport.ItemsSource = AirportList;
        }

        private void PopulateDict()
        {
            CarrierDict["AA"] = "American";
            CarrierDict["AS"] = "Alaska";
            CarrierDict["B6"] = "JetBlue";
            CarrierDict["DL"] = "Delta";
            CarrierDict["F9"] = "Frontier";
            CarrierDict["HA"] = "Hawaiian";
            CarrierDict["NK"] = "Spirit";
            CarrierDict["UA"] = "United";
            CarrierDict["US"] = "US Air";
            CarrierDict["VX"] = "Virgin America";
            CarrierDict["WN"] = "Southwest";
            CarrierDict["9E"] = "Pinnacle";
            CarrierDict["FL"] = "AirTran";
            CarrierDict["YV"] = "Mesa";
            CarrierDict["XE"] = "ExpressJet";
        }

        private void CreateURL(int querytype)
        {
            bool first = true;
            switch (querytype)
            {
                case 1:
                    _url = _emrUrl + "delay_statistics/?" ;
                    break;
                case 2:
                    _url = _emrUrl + "delay_carrier/?";
                    break;
                case 3:
                    _url = _emrUrl + "most_cancelled/?";
                    break;
                case 4:
                    _url = _emrUrl + "air_time/?";
                    break;
                case 5:
                    _url = _emrUrl + "delay_month/?";
                    break;
                case 6:
                    _url = _emrUrl + "delay_predict/?sd=" + TravelDate.SelectedDate.Value.ToString("yyyy-MM-dd") ;
                    first = false;
                    break;
                default:
                    break;
            }

            


            if (StartDate.SelectedDate != null && querytype != 6)
            {
                _url += "sd=" + StartDate.SelectedDate.Value.ToString("yyyy-MM-dd");
                first = false;       
            }

            if (EndDate.SelectedDate != null && querytype != 6)
            {
                _url += "&ed=" + EndDate.SelectedDate.Value.ToString("yyyy-MM-dd");
            }

            if ( comboBoxCarrier.SelectedItem != null)
            {
                
                _url += "&cr=" + CarrierDict.FirstOrDefault(x => x.Value == comboBoxCarrier.SelectedItem).Key ;
            }

            if (comboBoxOrigin.SelectedItem != null && querytype != 6)
            {
                if (first == false)
                {
                    _url = _url + "&";
                }
                _url += "oc=" + HttpUtility.UrlEncode(comboBoxOrigin.SelectedItem.ToString());

                first = false;
            }
            if (comboBoxDest.SelectedItem != null && querytype != 6)
            {
                if (first == false)
                {
                    _url = _url + "&";
                }
                _url += "dc=" + HttpUtility.UrlEncode(comboBoxDest.SelectedItem.ToString());
            }

            if (comboBoxOriginAirport.SelectedItem != null && querytype == 6)
            {
                if (first == false)
                {
                    _url = _url + "&";
                }
                _url += "oc=" + HttpUtility.UrlEncode(comboBoxOriginAirport.SelectedItem.ToString());

                first = false;
            }
            if (comboBoxDestAirport.SelectedItem != null && querytype == 6)
            {
                if (first == false)
                {
                    _url = _url + "&";
                }
                _url += "dc=" + HttpUtility.UrlEncode(comboBoxDestAirport.SelectedItem.ToString());
            }
        }

        

        private void Submit_Click(object sender, RoutedEventArgs e)
        {
           
            int selected = comboBoxQuery.SelectedIndex + 1;
            ResultPage.Content = null;
            ResultPage.NavigationService.RemoveBackEntry();
            Status.Text = "Loading................";
            switch (selected)
            {
                case 1:
                    CreateURL(selected);
                    ResultPage.Navigate(new Pages.Query1(_url));
                    break;
                case 2:
                    CreateURL(selected);
                    ResultPage.Navigate(new Pages.Query2(_url));
                    break;
                case 3:
                    CreateURL(selected);
                    ResultPage.Navigate(new Pages.Query3(_url));
                    break;
                case 4:
                    CreateURL(selected);
                    ResultPage.Navigate(new Pages.Query4(_url));
                    break;
                case 5:
                    CreateURL(selected);
                    ResultPage.Navigate(new Pages.Query5(_url));
                    break;
                case 6:
                    CreateURL(selected);
                    ResultPage.Navigate(new Pages.Query6(_url));
                    break;
                default:
                    break;
            }

            Status.Text = "";

        }
    }
}
