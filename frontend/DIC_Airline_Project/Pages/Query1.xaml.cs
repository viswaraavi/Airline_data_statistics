using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using RestSharp;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;
using System.Windows.Controls.DataVisualization.Charting;

namespace DIC_Airline_Project.Pages
{
    /// <summary>
    /// Interaction logic for Query1.xaml
    /// </summary>
    public partial class Query1 : Page
    {
        Entities.JsonServiceResult1 _result = new Entities.JsonServiceResult1();          
        public Query1(string url)
        {
            InitializeComponent();
            LoadJsonData(url);
            LoadPieChartData();
        }

        public void LoadJsonData(string url)
        {
            var client = new RestClient(url);
            var request = new RestRequest(Method.GET);
            request.OnBeforeDeserialization = resp => { resp.ContentType = "application/json"; };
            var response = client.Execute<List<Entities.JsonServiceResult1>>(request);
            string content = response.Content.ToString();

            content = content.Substring(1, content.Length - 2);
            content = content.Replace(@"\", "");
            _result =  JsonConvert.DeserializeObject<Entities.JsonServiceResult1>(content);                 
                      
        }
        private void LoadPieChartData()
        {
            
            ((PieSeries)mcChart.Series[0]).ItemsSource =
            new KeyValuePair<string, int>[]{
            new KeyValuePair<string, int>("carrier delay", (int)_result.carrer_delay),
            new KeyValuePair<string, int>("weather delay", (int)_result.weather_delay),
            new KeyValuePair<string, int>("national airsystem delay",(int)_result.nas_delay),
            new KeyValuePair<string, int>("security delay", (int)_result.security_delay),
            new KeyValuePair<string, int>("late aircraft delay", (int)_result.late_aircraft)
            };

            mcChart.Visibility = Visibility.Visible;
        }


    }
}
