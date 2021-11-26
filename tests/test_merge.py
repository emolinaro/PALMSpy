import pytest
import linecache

class Output:

    def __init__(self, path):
        self._path = path

    @property
    def path(self):
        return self._path

    def get_line_number(self):
        return len(open(self.path).readlines())

    def get_first_line(self):
        return linecache.getline(self.path, 2)

    def get_middle_line(self):
         return linecache.getline(self.path, self.get_line_number()//2)

    def get_last_line(self):
        return linecache.getline(self.path, self.get_line_number())

@pytest.fixture()
def output_gps_acc():
    _output = Output("PALMSpy_output/palmspy_gps_acc_all.csv")
    return _output

@pytest.fixture()
def output_acc_gps():
    _output = Output("PALMSpy_output/palmspy_acc_gps_all.csv")
    return _output

def test_merge_data_to_gps(output_gps_acc):
    assert output_gps_acc.get_line_number() == 388432
    assert output_gps_acc.get_first_line() == '1,2016-08-16 18:23:25,2,51.01609,-114.099305,0.0,0.0,0.0,2,65,1,0,4\n'
    assert output_gps_acc.get_middle_line() == '2,2016-07-04 13:20:45,1,51.045707,-114.070493,1.0712737590365733,1073.0,0.7713171065063328,1,0,0,0,29\n'
    assert output_gps_acc.get_last_line() == '3,2016-07-09 11:55:05,6,51.023118,-114.103123,0.0,1085.0,0.0,1,0,0,0,43\n'

def test_merge_data_to_acc(output_acc_gps):
    assert output_acc_gps.get_line_number() == 492238
    assert output_acc_gps.get_first_line() == '1,2016-08-15 21:35:00,325,2,0,0,,,,,,,\n'
    assert output_acc_gps.get_middle_line() == '2,2016-07-05 02:16:10,-2,-2,0,0,2,51.010387,-114.096742,0.0,1078.0,0.0,1\n'
    assert output_acc_gps.get_last_line() == '3,2016-07-11 09:02:30,0,0,0,0,,,,,,,\n'

