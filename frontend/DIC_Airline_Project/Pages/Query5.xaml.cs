using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.DataVisualization.Charting;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace DIC_Airline_Project.Pages
{
    /// <summary>
    /// Interaction logic for Query5.xaml
    /// </summary>
    public partial class Query5 : Page
    {
        List<Entities.JsonServiceResult5> _result = new List<Entities.JsonServiceResult5>();
        public Query5(string url)
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
            var response = client.Execute<List<Entities.JsonServiceResult5>>(request);
            string content = response.Content.ToString();

            content = content.Substring(1, content.Length - 2);
            content = "[" + content.Replace(@"\", "") + "]";
            _result = JsonConvert.DeserializeObject<List<Entities.JsonServiceResult5>>(content);
            _result = _result.OrderBy(si => si.MONTH).ToList();
            

        }
        private void LoadPieChartData()
        {
          
            ObservableCollection<KeyValuePair<string, int>> ValueList = new ObservableCollection<KeyValuePair<string, int>>();

            for (int i = 0; i < _result.Count; i++)
            {
                ValueList.Add(new KeyValuePair<string, int>((i+1).ToString(), (int)(double)_result[i].DEP_DELAY_AVG));
            }

            ((ColumnSeries)columnChart.Series[0]).ItemsSource = ValueList;

            ObservableCollection<KeyValuePair<string, int>> ValueList1 = new ObservableCollection<KeyValuePair<string, int>>();

            for (int i = 0; i < _result.Count; i++)
            {
                ValueList1.Add(new KeyValuePair<string, int>((i + 1).ToString(), (int)(double)_result[i].ARR_DELAY_AVG));
            }

            ((ColumnSeries)columnChart.Series[1]).ItemsSource = ValueList1;
            columnChart.Visibility = Visibility.Visible;
            

        }
    }
}
