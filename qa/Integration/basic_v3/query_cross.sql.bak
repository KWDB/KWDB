SET extra_float_digits = 0;
CREATE DATABASE rdb;
CREATE TABLE rdb.DeviceModel (
    ModelID int PRIMARY KEY,
    TypeID int,
    ModelName varchar(100),
    TypeName varchar(100)
);

INSERT INTO rdb.DeviceModel (modelID, TypeID, ModelName,TypeName)
SELECT x,1,'电表模型' || x::string, '电表'
FROM generate_series(1,5) AS t(x);

INSERT INTO rdb.DeviceModel (modelID, TypeID, ModelName,TypeName)
SELECT x,2,'变压器模型' || x::string, '变压器'
FROM generate_series(6,10) AS t(x);


CREATE TABLE rdb.Device (
    deviceID INT PRIMARY KEY,
    modelID INT,
    installDate DATE,
    location VARCHAR(255),
    FOREIGN KEY (modelID) REFERENCES rdb.DeviceModel(modelID)
);

INSERT into rdb.Device values(001,1,'2023-07-01','area1');
INSERT into rdb.Device values(002,2,'2023-07-01','area2');
INSERT into rdb.Device values(003,3,'2023-07-01','area3');
INSERT into rdb.Device values(004,4,'2023-07-02','area1');
INSERT into rdb.Device values(005,5,'2023-07-04','area1');
INSERT into rdb.Device values(006,1,'2023-07-03','area2');
INSERT into rdb.Device values(007,2,'2023-07-02','area2');
INSERT into rdb.Device values(008,3,'2023-07-04','area3');
INSERT into rdb.Device values(009,4,'2023-07-02','area3');
INSERT into rdb.Device values(010,5,'2023-07-04','area2');
INSERT into rdb.Device values(011,6,'2023-07-01','area2');
INSERT into rdb.Device values(012,7,'2023-07-03','area1');
INSERT into rdb.Device values(013,8,'2023-07-04','area1');
INSERT into rdb.Device values(014,9,'2023-07-02','area2');
INSERT into rdb.Device values(015,10,'2023-07-01','area1');
INSERT into rdb.Device values(016,6,'2023-07-04','area3');
INSERT into rdb.Device values(017,7,'2023-07-03','area1');
INSERT into rdb.Device values(018,8,'2023-07-02','area2');
INSERT into rdb.Device values(019,9,'2023-07-01','area2');
INSERT into rdb.Device values(020,10,'2023-07-04','area1');
INSERT into rdb.Device values(021,10,'2023-07-04','area1');


create ts DATABASE tsdb;
CREATE TABLE tsdb.MonitoringCenter (
    ts TIMESTAMP not NULL,
    deviceID INT,
    status INT
)tags (location varchar(64) not null,type varchar(64)) primary tags(location);

--truncate table tsdb.MonitoringCenter;
INSERT INTO tsdb.monitoringcenter VALUES ('2023-07-07 23:23:16.210909', 18, 0,'beijing','监控中心');
INSERT INTO tsdb.monitoringcenter VALUES ('2023-07-08 04:41:43.513525', 7, -1,'beijing','监控中心');
INSERT INTO tsdb.monitoringcenter VALUES ('2023-07-08 14:55:50.421123', 14, 0,'beijing','监控中心');
INSERT INTO tsdb.monitoringcenter VALUES ('2023-07-10 00:01:06.132851', 16, -1,'beijing','监控中心');
INSERT INTO tsdb.monitoringcenter VALUES ('2023-07-10 04:20:04.337247', 2, 0,'beijing','监控中心');

CREATE TABLE tsdb.MeterSuper (
    ts TIMESTAMP not NULL,
    current FLOAT, voltage FLOAT,
    frequency FLOAT ) TAGS (deviceID INT not null,modelId INT not null) primary tags(deviceID,modelId);

CREATE TABLE tsdb.TransformerSuper (
    ts TIMESTAMP not null ,
    current FLOAT,
    voltage FLOAT,
    frequency FLOAT,
    temperature FLOAT ) TAGS (deviceID INT not null,modelId INT not null) primary tags(deviceID,modelId);

INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 05:17:11.598796', 51.3657007868602, 168.1231095847923, 21.94992075582192,001,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 16:27:56.456342', 11.3623350383022, 152.52975579257537, 18.31554798487886,001,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 18:34:48.360415', 83.4972620588160, 109.48159492640826, 27.324761198065488,001,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 19:30:26.451473', 69.6333622281017, 238.1795385905312, 58.909419370691154,001,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 02:03:34.87516',  92.1571660223094, 37.317325295080366, 58.7212739638845,001,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 04:37:27.129072', 93.3225547467301, 66.65767426868712, 43.90722480110654,001,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 04:04:07.143652', 99.4678675025397, 13.025241892819679, 45.782854067873444,001,1);

INSERT INTO tsdb.MeterSuper VALUES ('2023-07-08 00:08:14.696399', 28.5696601355255, 140.69022448554762, 16.06097913378548,002,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 18:39:30.438076', 4.7143663137418, 73.30383143387422, 8.275714924556183,002,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 17:57:30.224437', 12.2075528531539, 129.54033282326606, 18.713733420164118,002,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 00:56:27.711008', 26.9682940704655, 221.25241776908553, 22.858753844306392,002,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 00:56:07.349385', 77.9899693670714, 193.14937841571833, 15.152851726883938,002,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 01:24:52.378961', 11.1845391378352, 39.46096627287062, 0.602579310396294,002,2);

INSERT INTO tsdb.MeterSuper VALUES ('2023-07-07 16:13:06.335841', 88.2676084108624, 151.86403428980412, 26.351079855814277,003,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-07 16:36:31.394578', 89.7564312801932, 150.9286388179774, 9.724153215641849,003,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-08 20:51:54.681879', 5.1943308789773, 63.650041383934024, 52.24253064057123,003,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 06:03:05.814713', 19.1017958876088, 115.68571470613875, 1.1906406929428215,003,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 18:29:18.422884', 83.6478628663485, 89.68909745421143, 37.59765999821056,003,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 03:45:32.968117', 92.1492678972427, 122.08378457767054, 27.42228598926843,003,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 05:33:16.169119', 42.5942338040329, 208.61578791825679, 42.02357324911631,003,3);


INSERT INTO tsdb.MeterSuper VALUES ('2023-07-08 06:14:37.170895', 64.7608834949323, 11.403069794532996, 6.998607666354104,004,4);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 08:51:17.852436', 86.6408620731391, 144.60721697795435, 50.97251993132019,004,4);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 02:55:40.980745', 50.3771483027861, 161.3108226729517, 57.6755664361319,004,4);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 05:48:09.816847', 70.4166998637219, 38.34458962405762, 16.85200336410034,004,4);


INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 15:08:47.661363', 38.3605591260021, 154.90060510274617, 47.726143882005516,005,5);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 22:48:17.624277', 55.5757147937953, 105.9471767852645, 29.46434611962374,005,5);


INSERT INTO tsdb.MeterSuper VALUES ('2023-07-08 04:18:54.2371',   38.7726102322915, 44.452551510062506, 37.929835054357994,006,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-08 23:01:41.986756', 8.0020595087802, 124.82361593824209, 53.82099555672923,006,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 22:23:27.094211', 21.2326354535832, 160.2957184770051, 46.04126608033752,006,1);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 00:48:26.702666', 27.4444017465370, 236.3482428866169, 14.455974850198032,006,1);


INSERT INTO tsdb.MeterSuper VALUES ('2023-07-08 09:56:25.220454', 32.2432444339341, 177.58638349576614, 3.3144646052362248,007,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 07:30:23.077706', 32.5922311651652, 168.70620355188322, 50.64488151265422,007,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 23:26:27.903319', 2.5362712857187, 184.42840348813348, 19.7483576372737,007,2);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 06:19:18.907957', 15.9094854493545, 178.5245342931026, 36.93833227226165,007,2);


INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 20:59:09.024885', 43.7369827209636, 68.60881166210703, 59.894456926545985,008,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 06:08:56.78358',  51.4086117972453, 177.3481559228597, 3.010382558704663,008,3);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 05:09:17.234702', 97.2639127907299, 23.33762706075646, 56.938319161331634,008,3);


INSERT INTO tsdb.MeterSuper VALUES ('2023-07-05 21:59:04.181575', 1.4552285651376, 113.32679246559366, 30.435887186907138,009,4);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-08 20:39:44.320729', 66.3840449793426, 182.2320607559655, 19.644153989567172,009,4);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 01:08:32.321363', 65.8289096339743, 195.69145838606715, 25.57179586013561,009,4);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 02:36:56.580835', 58.7603069416189, 159.2415972911047, 41.203189272752496,009,4);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 03:13:15.191151', 51.3779408142966, 205.9240838657982, 18.087854549500975,009,4);


INSERT INTO tsdb.MeterSuper VALUES ('2023-07-07 17:20:49.140594', 30.1632209734123, 172.39333958402796, 5.5044138372612395,010,5);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 08:29:58.162785', 79.6054423091845, 231.30201854016406, 4.597319670141218,010,5);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-09 11:58:38.783278', 35.0903702996475, 42.154668081937245, 54.7685818789391,010,5);
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-10 02:25:17.443994', 27.6364208231029, 154.3673060297445, 26.535563164735407,010,5);

INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 11:02:45.648432', 62.07842324087949, 90.32647676303242, 5.2488945576412505, 14.258898967639908,011,6);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 15:28:55.104231', 17.06637837986058, 142.44160208902713, 34.591380565349894, 21.119144923832494,011,6);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 04:58:21.719843', 92.25295471123367, 7.462731969278593, 50.92098064216266, 34.36563673165921,011,6);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-06 13:07:03.931919', 65.48081994336954, 188.2567527939338, 36.3655120695077, 3.4270151228768952,012,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 01:06:58.703196', 55.34867698580612, 72.40931028350673, 53.71553807319458, 3.7044275575844665,012,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 15:52:46.964902', 17.312386131568047, 195.57991222777645, 49.669131142369736, 10.483555552490174,012,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 21:43:54.889776', 85.58101910367206, 43.27803927302, 56.33795256303635, 38.31035323909134,012,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 04:08:50.57425', 17.398723556827278, 153.22633009602612, 11.45598498035433, 22.81971080834012,012,7);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-04 23:31:56.375171', 52.66458023829017, 90.29668184077906, 59.79066216659831, 29.722281663583914,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-07 08:03:17.177078', 43.96180865146384, 154.08019995405937, 18.43868808781785, 19.90639126594772,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-07 22:21:08.138838', 99.02138838252128, 223.71390545879052, 20.056195602708726, 15.238958313498614,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-08 16:59:52.728693', 60.54882232393375, 205.1858911185957, 19.85350199922358, 4.2600144450656785,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 20:33:49.564525', 15.245646381004008, 118.97958802680506, 5.033308167484023, 5.173988849891913,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 21:39:27.497345', 5.298795986209015, 163.30937120600368, 6.26727010942794, 7.4077283998133225,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 21:51:01.906044', 1.4700570958964931, 61.281910675128586, 20.885973192990406, 36.19385306158179,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 23:24:07.371084', 77.87625532148503, 112.8121161342392, 29.57175230887806, 3.7888085173906916,013,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 00:43:24.784041', 17.944334196761247, 62.09330906849601, 12.67792894618296, 16.370919752266246,013,8);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-08 00:12:44.891526', 29.141342330967035, 210.57264099760317, 55.87475999983724, 0.8872604651996596,014,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-08 17:02:57.967638', 55.861789989643995, 49.21382837566824, 10.050729726989687, 27.853727800951305,014,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-08 21:35:55.53145', 4.419608514891493, 128.98575536071803, 40.54320515432444, 35.673271601207546,014,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 12:59:55.047625', 10.658966522300162, 34.36371792002092, 36.054631460578506, 0.8882621813293667,014,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 12:53:43.162267', 0.832251979106502, 210.96849156906188, 7.079982839455994, 20.594480582398234,014,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 12:39:09.233924', 93.55229687398712, 221.38709657104926, 5.651785607203621, 30.081871012820898,014,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 01:32:00.465653', 56.33623462371027, 197.8153665633306, 0.12888790457715515, 19.984287824026694,014,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 00:59:30.129295', 40.284123214722456, 95.55812731279872, 30.168265976603976, 28.379620596021056,014,9);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-08 21:00:47.29422', 32.90240154925641, 7.325592204106499, 10.417407156383973, 8.326718062503033,015,10);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 21:14:38.358985', 66.9504922275415, 31.954296677230047, 52.917001772298136, 16.144134625639452,015,10);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 04:50:14.701774', 51.74081587970605, 225.54199375949992, 33.6010026825349, 9.984459670006203,015,10);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 06:17:09.386495', 82.79836774723499, 74.3088997740017, 14.858983253662146, 16.935362895877404,015,10);

INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-08 16:14:12.921932', 36.09092394088478, 108.39520722419337, 25.335029508743503, 38.21809137759246,016,6);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 00:30:42.935178', 20.78304951765162, 215.61307198766684, 54.555908978046546, 4.02753975120433,016,6);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 19:41:34.607783', 35.83891266157195, 123.09969886987602, 3.8893447610068677, 19.25033076157277,016,6);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 03:04:43.064627', 25.060728129290055, 168.2589500097859, 38.01445660686667, 12.739341182518302,016,6);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 03:06:00.512731', 50.771485089709145, 144.99413205257952, 58.65062395303319, 5.220398496930159,016,6);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 05:38:07.210942', 11.986872694219386, 120.1265712304425, 43.56598628018105, 16.070285819936743,016,6);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-05 19:06:03.818329', 24.987985173545724, 72.9023828811242, 16.973064797835633, 36.293677843691654,017,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-07 15:25:08.655828', 80.70288139874826, 226.76149960639748, 22.536435359695517, 17.602529048121482,017,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 16:03:27.033861', 87.27905262816087, 206.0703472669354, 19.650004737152145, 23.924112292515645,017,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 19:28:09.125524', 91.5020840605461, 220.6248576001525, 22.413093079713846, 31.84150123087136,017,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 19:16:34.71569', 79.06313290189892, 236.1186641981095, 11.429809386155725, 1.0275678498294383,017,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 19:19:38.862837', 3.273131553499198, 70.03972367014512, 58.98496325869537, 37.186032602606076,017,7);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 19:19:44.244468', 37.8918817740594, 161.35502082836354, 58.180074981489085, 14.735220601556307,017,7);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-07 23:53:58.049632', 25.080051494014555, 131.9743131559457, 34.81001821039513, 29.42230105166729,018,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 11:04:00.447746', 85.23375508716988, 19.436503499836135, 37.700593790189956, 8.991060302139147,018,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 20:05:51.09422', 88.20280845651922, 97.64938995519202, 38.45793498252661, 20.427684033114843,018,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 21:14:41.547481', 53.912778954858354, 108.51135844595831, 39.49629354672162, 12.141814707802041,018,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 00:31:23.234403', 91.83096434802565, 98.20607678802872, 8.917065537194588, 14.635690609361092,018,8);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 01:53:16.059696', 90.7270041420773, 47.37665748498699, 58.14147201029144, 21.628951126758835,018,8);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 23:54:00.036213', 91.90978517570159, 89.82349051872717, 9.816515594798574, 28.385429384530454,019,9);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 06:06:57.554089', 73.23775705733766, 233.65862651811682, 18.125960923250233, 15.367487286523698,019,9);


INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-07 01:58:30.973539', 22.71351816345728, 20.663093027703496, 50.3940697076095, 15.98260864725475,020,10);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-07 16:34:00.391646', 45.23884339183191, 99.00457899838102, 56.80338537583516, 8.004371769193597,020,10);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-09 16:03:06.617241', 31.504070234340986, 127.98633740808924, 8.428259533187799, 37.91307500839949,020,10);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 06:16:57.57278', 19.012564333445425, 200.65811585260263, 7.407489418857125, 1.784382784184828,020,10);
INSERT INTO tsdb.TransformerSuper VALUES ('2023-07-10 05:32:26.735818', 34.23666579094089, 136.83684208696292, 42.117931529613344, 14.656870311446397,020,10);

SELECT COUNT(*) from tsdb.MeterSuper;

SELECT COUNT(*) from tsdb.TransformerSuper;

--correlated subqueries
-- Get the device installation date and the last voltage reading for all electricity meters (MeterSuper)
SELECT d.deviceID, d.installDate, ms.voltage
FROM rdb.Device AS d
INNER JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
WHERE ms.ts = (SELECT MAX(ts) FROM tsdb.MeterSuper WHERE deviceID = d.deviceID)
ORDER BY d.deviceID;

-- Get the device type and device model of all devices with status -1
SELECT d.deviceID, dm.TypeName, dm.ModelName
FROM rdb.Device AS d
INNER JOIN rdb.DeviceModel AS dm ON d.modelID = dm.modelID
INNER JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
WHERE mc.status = -1
ORDER BY d.deviceID;

-- bug  apply join
-- Get the device installation location and the last temperature reading of all transformers (TransformerSuper)
SELECT d.location, ts.temperature
FROM rdb.Device AS d
INNER JOIN tsdb.TransformerSuper AS ts ON d.deviceID = ts.deviceID
WHERE ts.ts = (SELECT MAX(ts) FROM tsdb.TransformerSuper WHERE deviceID = d.deviceID)
ORDER BY d.deviceID;

-- Get the device model name and the last current reading of all meters (MeterSuper)
SELECT d.deviceID, dm.ModelName, ms.current
FROM rdb.Device AS d
INNER JOIN rdb.DeviceModel AS dm ON d.modelID = dm.modelID
INNER JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
WHERE ms.ts = (SELECT MAX(ts) FROM tsdb.MeterSuper WHERE deviceID = d.deviceID)
ORDER BY d.deviceID;


--Non-correlated subqueries
-- Get the device ID, device model, and its last status (if any) for all devices
SELECT d.deviceID, dm.ModelName, mc.status
FROM rdb.Device AS d
LEFT JOIN rdb.DeviceModel AS dm ON d.modelID = dm.modelID
LEFT JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
WHERE mc.ts = (SELECT MAX(ts) FROM tsdb.MonitoringCenter WHERE deviceID = d.deviceID)
ORDER BY d.deviceID;

-- Get the model name of all device models and the last voltage reading of the corresponding device (if any)
SELECT d.deviceID, dm.ModelName, ms.voltage
FROM rdb.DeviceModel AS dm
LEFT JOIN rdb.Device AS d ON dm.modelID = d.modelID
LEFT JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
WHERE ms.ts = (SELECT MAX(ts) FROM tsdb.MeterSuper WHERE deviceID = d.deviceID)
ORDER BY d.deviceID;

-- Get the device ID, installation location, and last temperature reading (if any) for all devices
SELECT d.deviceID, d.location, ts.temperature
FROM rdb.Device AS d
LEFT JOIN tsdb.TransformerSuper AS ts ON d.deviceID = ts.deviceID
WHERE ts.ts = (SELECT MAX(ts) FROM tsdb.TransformerSuper WHERE deviceID = d.deviceID)
ORDER BY d.deviceID;

-- Get the model name of all device models and the last current reading of the corresponding device (if any)
SELECT d.deviceID, dm.ModelName, ms.current
FROM rdb.DeviceModel AS dm
LEFT JOIN rdb.Device AS d ON dm.modelID = d.modelID
LEFT JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
WHERE ms.ts = (SELECT MAX(ts) FROM tsdb.MeterSuper WHERE deviceID = d.deviceID)
ORDER BY d.deviceID;


--Correlated subqueries
-- Get the device status of all monitoring centers and their corresponding device models (if any)
SELECT d.deviceID, mc.status, dm.ModelName
FROM rdb.DeviceModel AS dm
RIGHT JOIN rdb.Device AS d ON dm.modelID = d.modelID
RIGHT JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
ORDER BY d.deviceID, mc.status, dm.ModelName;

-- Get the voltage readings of all electricity meters (MeterSuper) and their corresponding equipment installation date (if any)
SELECT d.deviceID, ms.voltage, d.installDate
FROM rdb.Device AS d
RIGHT JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
ORDER BY d.deviceID, ms.voltage, d.installDate;

-- Get the temperature readings of all transformers (TransformerSuper) and their corresponding equipment installation locations (if any)
SELECT d.deviceID, ts.temperature, d.location
FROM rdb.Device AS d
RIGHT JOIN tsdb.TransformerSuper AS ts ON d.deviceID = ts.deviceID
ORDER BY d.deviceID, ts.temperature, d.location;

-- Get the current readings of all meters (MeterSuper) and their corresponding device models (if any)
SELECT d.deviceID, ms.current, dm.ModelName
FROM rdb.DeviceModel AS dm
RIGHT JOIN rdb.Device AS d ON dm.modelID = d.modelID
RIGHT JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
ORDER BY d.deviceID, ms.current, dm.ModelName;


--Correlated subqueries
-- Get the status of all devices and their corresponding device models (if any)
SELECT d.deviceID, mc.status, dm.ModelName
FROM rdb.DeviceModel AS dm
FULL JOIN rdb.Device AS d ON dm.modelID = d.modelID
FULL JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
ORDER BY d.deviceID, mc.status, dm.ModelName;

-- Get the voltage readings of all electricity meters (MeterSuper) and their corresponding equipment installation date (if any)
SELECT d.deviceID, ms.voltage, d.installDate
FROM rdb.Device AS d
FULL JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
ORDER BY d.deviceID, ms.voltage, d.installDate;

-- Get the temperature readings of all transformers (TransformerSuper) and their corresponding equipment installation locations (if any)
SELECT d.deviceID, ts.temperature, d.location
FROM rdb.Device AS d
FULL JOIN tsdb.TransformerSuper AS ts ON d.deviceID = ts.deviceID
ORDER BY d.deviceID, ts.temperature, d.location;

-- Get the current readings of all meters (MeterSuper) and their corresponding device models (if any)
SELECT d.deviceID, ms.current, dm.ModelName
FROM rdb.DeviceModel AS dm
FULL JOIN rdb.Device AS d ON dm.modelID = d.modelID
FULL JOIN tsdb.MeterSuper AS ms ON d.deviceID = ms.deviceID
ORDER BY d.deviceID, ms.current, dm.ModelName;

--The subquery must appear in the select list

--Query the number of devices for each device model.


SELECT ModelName,
       (SELECT COUNT(*) FROM rdb.Device WHERE modelID = dm.modelID) AS DeviceCount
FROM rdb.DeviceModel AS dm ORDER by ModelName;


--Query the latest device installation date for each device model.


SELECT ModelName,
       (SELECT MAX(installDate) FROM rdb.Device WHERE modelID = dm.modelID) AS LatestInstallDate
FROM rdb.DeviceModel AS dm ORDER by ModelName;


--Query each device for the most recent current reading.


SELECT d.deviceID,
       (SELECT MAX(current) FROM tsdb.MeterSuper WHERE deviceID = d.deviceID) AS LatestCurrent
FROM rdb.Device AS d ORDER by d.deviceID;


--Query each device for the most recent voltage reading.


SELECT d.deviceID,
       (SELECT MAX(voltage) FROM tsdb.MeterSuper WHERE deviceID = d.deviceID) AS LatestVoltage
FROM rdb.Device AS d ORDER by d.deviceID;


--Query each device for the most recent frequency reading.


SELECT d.deviceID,
       (SELECT MAX(frequency) FROM tsdb.MeterSuper WHERE deviceID = d.deviceID) AS LatestFrequency
FROM rdb.Device AS d ORDER by d.deviceID;


--Query the latest status of each device.


SELECT d.deviceID,
       (SELECT MAX(status) FROM tsdb.MonitoringCenter WHERE deviceID = d.deviceID) AS LatestStatus
FROM rdb.Device AS d ORDER by d.deviceID;


--Query the latest transformer temperature reading for each device.


SELECT d.deviceID,
       (SELECT MAX(temperature) FROM tsdb.TransformerSuper WHERE deviceID = d.deviceID) AS LatestTemperature
FROM rdb.Device AS d ORDER by d.deviceID;


--Query the average current reading for each device model.


SELECT ModelName,
       (SELECT AVG(current) FROM tsdb.MeterSuper WHERE deviceID IN 
            (SELECT deviceID FROM rdb.Device WHERE modelID = dm.modelID)) AS AvgCurrent
FROM rdb.DeviceModel AS dm ORDER by ModelName;


--Query the average voltage reading for each device model.


SELECT ModelName,
       (SELECT AVG(voltage) FROM tsdb.MeterSuper WHERE deviceID IN 
            (SELECT deviceID FROM rdb.Device WHERE modelID = dm.modelID)) AS AvgVoltage
FROM rdb.DeviceModel AS dm ORDER by ModelName;


--Query the average frequency reading for each device model.


SELECT ModelName,
       (SELECT AVG(frequency) FROM tsdb.MeterSuper WHERE deviceID IN 
            (SELECT deviceID FROM rdb.Device WHERE modelID = dm.modelID)) AS AvgFrequency
FROM rdb.DeviceModel AS dm ORDER by ModelName;




--The subquery must appear in the where clause

--Query detailed information about devices with monitoring data within a certain period of time:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MonitoringCenter 
    WHERE ts BETWEEN '2023-07-02' AND '2023-07-05'
) ORDER by deviceID,modelID,installDate,location;


--Query detailed information of electric meter devices whose average current exceeds a certain threshold:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MeterSuper 
    WHERE current > 10
) ORDER by deviceID,modelID,installDate,location;


--Query the monitoring status of all devices of a certain device model:

 SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE modelID = 1
) ORDER by deviceID,status,ts;


--To query detailed information about a device in a fault state:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MonitoringCenter 
    WHERE status = -1
) ORDER by deviceID,modelID,installDate,location;


--Query detailed information of transformer devices whose voltage exceeds a certain threshold:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.TransformerSuper 
    WHERE voltage > 110
) ORDER by deviceID,modelID,installDate,location;


--Query the monitoring data of devices whose installation dates are within a certain range:

SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE installDate BETWEEN '2023-07-01' AND '2023-07-05'
) ORDER by deviceID,status,ts;


--Query the monitoring data of all devices in a certain area:

SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE location = 'area1'
)ORDER by deviceID,status,ts;


--To query detailed information about faulty devices in a certain area:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MonitoringCenter 
    WHERE status = -1
) AND location = 'area1' ORDER by deviceID,modelID,installDate,location;


--Query detailed information about devices whose voltage exceeds a certain threshold within a certain period of time:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MeterSuper 
    WHERE voltage > 110 AND ts BETWEEN '2023-07-02' AND '2023-07-07'
) ORDER by deviceID,modelID,installDate,location;


--Query the monitoring data of all devices of a device model in a certain area:

SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE modelID IN (
        SELECT modelID FROM rdb.DeviceModel
        WHERE TypeName = '电表'
    ) AND location = 'area1'
)ORDER by deviceID,status,ts;





--Subquery in having SQL statement


--Query the average current of all devices, find the devices whose average current exceeds the overall average current obtained by the subquery, and sort them by device ID.

SELECT rdb.Device.deviceID, AVG(tsdb.MeterSuper.current) AS avg_current
FROM rdb.Device, tsdb.MeterSuper
WHERE rdb.Device.deviceID = tsdb.MeterSuper.deviceID
GROUP BY rdb.Device.deviceID
HAVING AVG(tsdb.MeterSuper.current) > 
  (SELECT AVG(current) FROM tsdb.MeterSuper)
ORDER BY rdb.Device.deviceID;

--Query the average voltage of all devices, find the devices whose average voltage is lower than the overall average voltage obtained by the subquery, and sort them by device ID.

SELECT rdb.Device.deviceID, AVG(tsdb.MeterSuper.voltage) AS avg_voltage
FROM rdb.Device, tsdb.MeterSuper
WHERE rdb.Device.deviceID = tsdb.MeterSuper.deviceID
GROUP BY rdb.Device.deviceID
HAVING AVG(tsdb.MeterSuper.voltage) < 
  (SELECT AVG(voltage) FROM tsdb.MeterSuper)
ORDER BY rdb.Device.deviceID;

--Query the average frequency of all devices, find the devices whose average frequency exceeds the overall average frequency obtained by the subquery, and sort them by device ID.

SELECT rdb.Device.deviceID, AVG(tsdb.MeterSuper.frequency) AS avg_frequency
FROM rdb.Device, tsdb.MeterSuper
WHERE rdb.Device.deviceID = tsdb.MeterSuper.deviceID
GROUP BY rdb.Device.deviceID
HAVING AVG(tsdb.MeterSuper.frequency) > 
  (SELECT AVG(frequency) FROM tsdb.MeterSuper)
ORDER BY rdb.Device.deviceID;

-- Query the maximum temperature of all devices, find the devices whose maximum temperature is lower than the overall maximum temperature obtained by the subquery, and sort them by device ID.

SELECT rdb.Device.deviceID, MAX(tsdb.TransformerSuper.temperature) AS max_temperature
FROM rdb.Device, tsdb.TransformerSuper
WHERE rdb.Device.deviceID = tsdb.TransformerSuper.deviceID
GROUP BY rdb.Device.deviceID
HAVING MAX(tsdb.TransformerSuper.temperature) < 
  (SELECT MAX(temperature) FROM tsdb.TransformerSuper)
ORDER BY rdb.Device.deviceID;

--Query the minimum temperature of all devices, find the devices whose minimum temperature is higher than the overall minimum temperature obtained by the subquery, and sort them by device ID.

SELECT rdb.Device.deviceID, MIN(tsdb.TransformerSuper.temperature) AS min_temperature
FROM rdb.Device, tsdb.TransformerSuper
WHERE rdb.Device.deviceID = tsdb.TransformerSuper.deviceID
GROUP BY rdb.Device.deviceID
HAVING MIN(tsdb.TransformerSuper.temperature) > 
  (SELECT MIN(temperature) FROM tsdb.TransformerSuper)
ORDER BY rdb.Device.deviceID;

--Query whether the average current of the meter with meter model 3 is higher than the average current of all meters. The results are sorted by timestamp.

SELECT m.ts, AVG(m.current) AS avg_current
FROM tsdb.MeterSuper as m, rdb.Device
WHERE m.deviceID=3 and m.modelID=3 and rdb.Device.deviceID = 3 AND rdb.Device.modelID = 3
GROUP BY m.ts
HAVING AVG(m.current) >
  (SELECT AVG(current) FROM tsdb.MeterSuper)
ORDER BY m.ts;

--Query the maximum voltage of the meter with meter model 2 to see whether it is lower than the maximum voltage of all meters. The results are sorted by timestamp.

SELECT m.ts, MAX(m.voltage) AS max_voltage
FROM tsdb.MeterSuper as m, rdb.Device
WHERE m.deviceID=2 and m.modelID=2 and rdb.Device.deviceID = 2 AND rdb.Device.modelID = 2
GROUP BY m.ts
HAVING MAX(m.voltage) <
  (SELECT MAX(voltage) FROM tsdb.MeterSuper)
ORDER BY m.ts;

--Query whether the lowest frequency of the transformer with transformer model 7 is higher than the lowest frequency of all transformers. The results are sorted by timestamp.

SELECT t.ts, MIN(t.frequency) AS min_frequency
FROM tsdb.TransformerSuper as t, rdb.Device
WHERE t.deviceID=17 and t.modelID=7 and rdb.Device.deviceID = 7 AND rdb.Device.modelID = 7
GROUP BY t.ts
HAVING MIN(t.frequency) >
  (SELECT MIN(frequency) FROM tsdb.TransformerSuper)
ORDER BY t.ts;

--Query whether the average temperature of the transformer with transformer model 8 is lower than the average temperature of all transformers. The results are sorted by timestamp.

SELECT t.ts, AVG(t.temperature) AS avg_temperature
FROM tsdb.TransformerSuper as t, rdb.Device
WHERE t.deviceID=18 and t.modelID=8 and rdb.Device.deviceID = 8 AND rdb.Device.modelID = 8
GROUP BY t.ts
HAVING AVG(t.temperature) <
  (SELECT AVG(temperature) FROM tsdb.TransformerSuper)
ORDER BY t.ts;

--Query whether the maximum current of the transformer with transformer model 10 is higher than the maximum current of all transformers. The results are sorted by timestamp.

SELECT t.ts, MAX(t.current) AS max_current
FROM tsdb.TransformerSuper as t, rdb.Device
WHERE t.deviceID=20 and t.modelID=10 and rdb.Device.deviceID = 10 AND rdb.Device.modelID = 10
GROUP BY t.ts
HAVING MAX(t.current) >
  (SELECT MAX(current)

 FROM tsdb.TransformerSuper)
ORDER BY t.ts;

--Subquery in exists

--Query the device model of the device instance in the MeterSuper table of the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = dm.modelID
) order by typename;


--Query the region where the device instances that exist in the Device table of the rdb library and have abnormal status in the MonitoringCenter table of the tsdb library are located:

SELECT d.location
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    AND mc.status = -1
) ORDER by d.location;


--Query the device model and region of device instances that exist in the Device table in the rdb library and have a current greater than 100 in the MeterSuper table in the tsdb library:

SELECT dm.TypeName, d.location
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.ModelID = d.modelID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
    AND ms.current > 100
);


--Query the device instances that exist in the MeterSuper table in the tsdb library, and the installation date of the same device instance exists in the Device table in the rdb library:

SELECT ms.deviceID, ms.ts
FROM tsdb.MeterSuper ms
WHERE EXISTS (
    SELECT 1
    FROM rdb.Device d
    WHERE d.deviceID = ms.deviceID
    AND d.installDate = ms.ts::date
) ORDER by ts;


--The query exists in the DeviceModel table of the rdb library, and there are device models and temperatures of device instances with the same device model in the TransformerSuper table of the tsdb library:

SELECT dm.TypeName, ts.temperature
FROM rdb.DeviceModel dm
JOIN tsdb.TransformerSuper ts ON dm.ModelID = ts.modelId
WHERE EXISTS (
    SELECT 1
    FROM rdb.Device d
    WHERE d.modelID = dm.ModelID
    AND d.deviceID = ts.deviceID
) ORDER by dm.TypeName, ts.temperature;


--The query exists in the Device table of the rdb library, and the device model and current of the device instance with a voltage greater than 200 exists in the MeterSuper table of the tsdb library: The relationship cannot be queried

SELECT dm.TypeName
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.ModelID = d.modelID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
    AND ms.voltage > 200
);


--The query exists in the Device table of the rdb library, and there is a device model and frequency of the device instance with the same device model in the TransformerSuper table of the tsdb library:

SELECT dm.TypeName, ts.frequency
FROM rdb.DeviceModel dm
JOIN tsdb.TransformerSuper ts ON dm.ModelID = ts.modelId
WHERE EXISTS (
    SELECT 1
    FROM rdb.Device d
    WHERE
 d.modelID = dm.ModelID
    AND d.deviceID = ts.deviceID
) ORDER by dm.TypeName, ts.frequency;

--The query exists in the Device table of the rdb library, and there are device models with the same device instance whose number of device instances is greater than 5 in the MonitoringCenter table of the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.ModelID = d.modelID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    GROUP BY mc.deviceID
    HAVING COUNT(*) > 5
);


--Query the device instances that exist in the MeterSuper table of the tsdb library, and the device models that have the same device instance and the number of device instances in the Device table of the rdb library is greater than or equal to 3:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper ms
    JOIN rdb.Device d ON d.deviceID = ms.deviceID
    WHERE d.modelID = dm.ModelID
    GROUP BY ms.deviceID
    HAVING COUNT(*) >= 3
);


--Get the model name, type name and corresponding current data of all devices, sorted in ascending order by device ID:

SELECT dm.ModelName, dm.TypeName, m.current
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
WHERE EXISTS (
    SELECT 1
    FROM rdb.Device d
    WHERE d.modelID = dm.modelID
)
ORDER BY m.deviceID ASC,current;


--Get the installation date, temperature, and corresponding voltage data of the transformer equipment, sorted in descending order by equipment ID:

SELECT d.installDate, t.temperature, t.voltage
FROM rdb.Device d
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
WHERE EXISTS (
    SELECT 1
    FROM rdb.DeviceModel dm
    WHERE dm.modelID = d.modelID
        AND dm.TypeName = '变压器'
)
ORDER BY d.deviceID DESC,voltage;


--Get the location, current, and corresponding frequency data of the meter device, and sort them in ascending order by location name:

SELECT d.location, m.current, m.frequency
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
WHERE EXISTS (
    SELECT 1
    FROM rdb.DeviceModel dm
    WHERE dm.modelID = d.modelID
        AND dm.TypeName = '电表'
)
ORDER BY d.location ASC,current;


--Get the model name and corresponding status of the device with status data, and sort it in descending order by status value:

SELECT dm.ModelName, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MonitoringCenter mc ON dm.modelID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc2
    WHERE mc2.deviceID = mc.deviceID
)
ORDER BY mc.status DESC;


--Get the model names and corresponding temperatures of transformer devices whose installation date is earlier than '2023-07-01' and whose temperature data exists, and sort them in descending order by installation date:

SELECT dm.ModelName, t.temperature
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.modelID = d.modelID
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
WHERE d.installDate < '2023-07-01'
    AND EXISTS (
        SELECT 1
        FROM tsdb.TransformerSuper t2
        WHERE t2.deviceID = t.deviceID
    )
ORDER BY d.installDate DESC;


--Get the device model name and the corresponding current and status data, and sort them in descending order of current and ascending order of status:

SELECT dm.ModelName, m.current, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper m2
    WHERE

 m2.deviceID = m.deviceID
)
ORDER BY m.current DESC, mc.status ASC;


--Get the current and frequency data of the electric meter device at location 'area1', and sort them in ascending order of frequency:

SELECT m.current, m.frequency
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
WHERE d.location = 'area1'
    AND EXISTS (
        SELECT 1
        FROM rdb.DeviceModel dm
        WHERE dm.modelID = d.modelID
            AND dm.TypeName = '电表'
    )
ORDER BY m.frequency ASC;


--Get the installation date and voltage data of transformer devices with a temperature greater than 50 and current data, and sort them in descending order of voltage:

SELECT d.installDate, t.voltage
FROM rdb.Device d
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
WHERE t.temperature > 50
    AND EXISTS (
        SELECT 1
        FROM tsdb.TransformerSuper t2
        WHERE t2.deviceID = t.deviceID
    )
ORDER BY t.voltage DESC;


--Get the model name and the corresponding current and status data, where the current is greater than 100 or the status is 1, and sort by current in descending order and status in ascending order:

SELECT dm.ModelName, m.current, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper m2
    WHERE m2.deviceID = m.deviceID
)
    AND (m.current > 100 OR mc.status = 1)
ORDER BY m.current DESC, mc.status ASC;


--Get the installation date and corresponding voltage data of the electric meter devices with location 'area2' and existence status 0, and sort them in descending order by installation date:

SELECT d.installDate, m.voltage
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE d.location = 'area2'
    AND mc.status = 0
    AND EXISTS (
        SELECT 1
        FROM rdb.DeviceModel dm
        WHERE dm.modelID = d.modelID
            AND dm.TypeName = '电表'
    )
ORDER BY d.installDate DESC;


--Subquery in in


--Query the device model of the device instance that exists in the Device table in the rdb library and in the MeterSuper table in the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
WHERE dm.ModelID IN (
    SELECT d.modelID
    FROM rdb.Device d
    WHERE d.deviceID IN (
        SELECT ms.deviceID
        FROM tsdb.MeterSuper ms
    )
);


--Query the device instances that exist in the MeterSuper table in the tsdb library and the installation date of the device instances that also exist in the Device table in the rdb library:

SELECT ms.deviceID, ms.ts
FROM tsdb.MeterSuper ms
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.deviceID IN (
        SELECT ms.deviceID
        FROM tsdb.MeterSuper ms
    )
    AND d.installDate = ms.ts::date
);


--Query the device model and region of device instances that exist in the DeviceModel table in the rdb library and in the MeterSuper table in the tsdb library and have a current greater than 100:

SELECT dm.TypeName, d.location
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.ModelID = d.modelID
WHERE dm.ModelID IN (
    SELECT ms.modelId
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID IN (
        SELECT d.deviceID
        FROM rdb.Device d
    )
    AND ms.current > 100
);


--The query exists in the DeviceModel table of the rdb library, and there are device models and temperatures of device instances with the same device model in the TransformerSuper table of the tsdb library:

SELECT dm.TypeName, ts.temperature
FROM rdb.DeviceModel dm
JOIN tsdb.TransformerSuper ts ON dm.ModelID = ts.modelId
WHERE dm.ModelID IN (
    SELECT ts.modelId
    FROM tsdb.TransformerSuper ts
    WHERE ts.deviceID IN (
        SELECT d.deviceID
        FROM rdb.Device d
    )
) order by ts.temperature;


--Query the device instances in the MeterSuper table in the tsdb library and the device models and currents of the device instances in the Device table in the rdb library:

SELECT dm.TypeName, ms.current
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.ModelID = d.modelID
JOIN tsdb.MeterSuper ms ON d.deviceID = ms.deviceID AND dm.ModelID = ms.modelId
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
) order by current;



--Get the current and frequency data of all electric meter devices with the model 'electric meter model 1', and sort them in descending order by current:

SELECT m.current, m.frequency
FROM tsdb.MeterSuper m
WHERE m.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
    WHERE dm.ModelName = '电表模型1'
)
ORDER BY m.current DESC;


--Get the temperature and corresponding voltage data of transformer devices with installation date after '2023-07-01', and sort them in ascending order by temperature:

SELECT t.temperature, t.voltage
FROM tsdb.TransformerSuper t
WHERE t.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
    WHERE dm.TypeName = '变压器'
        AND d.installDate > '2023-07-01'
)
ORDER BY t.temperature ASC;


--Get the installation date and voltage data of the electric meter devices with current data in location 'area1', and sort them in descending order by installation date:

SELECT d.installDate, m.voltage
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
WHERE d.location = 'area1'
    AND d.deviceID IN (
        SELECT m2.deviceID
        FROM tsdb.MeterSuper m2
    )
ORDER BY d.installDate DESC, voltage;


--Get the temperature and frequency data of the transformer device with model name 'Transformer Model 9', sorted by temperature in descending order:

SELECT t.temperature, t.frequency
FROM tsdb.TransformerSuper t
WHERE t.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
    WHERE dm.ModelName = '变压器模型9'
)
ORDER BY t.temperature DESC;



--Get the model name, type name, and corresponding current and status data of all devices, sorted in ascending order by device ID:

SELECT dm.ModelName, dm.TypeName, m.current, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM rdb.Device d
    WHERE d.modelID = dm.modelID
)
ORDER BY m.deviceID ASC, m.current;


--Get the location, current, and corresponding temperature data of the electric meter equipment, where the current is greater than 100, and sort in descending order by location name:

SELECT d.location, m.current, t.temperature
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
WHERE EXISTS (
    SELECT 1
    FROM rdb.DeviceModel dm
    WHERE dm.modelID = d.modelID
        AND dm.TypeName = '电表'
)
    AND m.current > 100
ORDER BY d.location DESC;


--Get the model names and corresponding current data of devices with existence status 1, and sort them in ascending order by model name:

SELECT dm.ModelName, m.current
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = m.deviceID
        AND mc.status = 1
)
ORDER BY dm.ModelName ASC;


--Get the installation date and corresponding temperature data of the transformer equipment, where the temperature is greater than 50, and sort in ascending order by installation date:

SELECT d.installDate, t.temperature
FROM rdb.Device d
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
WHERE EXISTS (
    SELECT 1
    FROM rdb.DeviceModel dm
    WHERE dm.modelID = d.modelID
        AND dm.TypeName = '变压器'
)
    AND t.temperature > 50
ORDER BY d.installDate ASC;


--Get the model name and corresponding current data of the device with position 'area2' and existence status 0, and sort them in descending order by current:

SELECT dm.ModelName, m.current
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE d.location = 'area2'
    AND mc.status = 0
ORDER BY m.current DESC;


--Get the location and corresponding voltage data of devices with installation date after '2023-05-01' and existence status 1, sorted by voltage in ascending order:

SELECT d.location, m.voltage
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE d.installDate > '2023-05-01'
    AND mc.status = 1
ORDER BY m.voltage ASC;


--The scalar subquery must appear in the select list.


--Query the current of each device instance in the MeterSuper table of the tsdb library and the average current of the same device instance in the Device table of the rdb library:

SELECT ms.deviceID, ms.current,
    (SELECT AVG(ms2.current)
     FROM tsdb.MeterSuper ms2
     WHERE ms2.deviceID = ms.deviceID) AS AverageCurrent
FROM tsdb.MeterSuper ms order by ms.deviceID,current;


--Query the device model of each device instance in the Device table of the rdb library and the maximum current of the same device instance in the MeterSuper table of the tsdb library:

SELECT dm.TypeName,
    (SELECT MAX(ms.current)
     FROM tsdb.MeterSuper ms
     WHERE ms.deviceID = d.deviceID) AS MaxCurrent
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID order by MaxCurrent;


--Query the device ID of each device instance in the MeterSuper table of the tsdb library and the installation date of the same device instance in the Device table of the rdb library:

SELECT ms.deviceID,
    (SELECT d.installDate
     FROM rdb.Device d
     WHERE d.deviceID = ms.deviceID) AS InstallDate
FROM tsdb.MeterSuper ms order by ms.deviceID;


--Query the device ID of each device instance in the Device table of the rdb library and the number of states of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT COUNT(mc.status)
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID) AS StatusCount
FROM rdb.Device d order by d.deviceID;


--Query the device ID of each device instance in the MeterSuper table of the tsdb library and the region name of the same device instance in the Device table of the rdb library:

SELECT ms.deviceID,
    (SELECT d.location
     FROM rdb.Device d
     WHERE d.deviceID = ms.deviceID) AS Location
FROM tsdb.MeterSuper ms order by ms.deviceID;


--Query the device ID of each device instance in the Device table of the rdb library and the latest status of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT mc.status
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID
     ORDER BY mc.ts DESC
     LIMIT 1) AS LatestStatus
FROM rdb.Device d order by deviceid;


--Query the device ID of each device instance in the MeterSuper table of the tsdb library and the device model of the same device instance in the Device table of the rdb library:

SELECT ms.deviceID,
    (SELECT dm.TypeName
     FROM rdb.DeviceModel dm
     WHERE dm.ModelID = (
         SELECT d.modelID
         FROM rdb.Device d
         WHERE d.deviceID = ms.deviceID
     )) AS TypeName
FROM tsdb.MeterSuper ms order by ms.deviceID;


--Query the device ID of each device instance in the Device table of the rdb library and the earliest status time of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT MIN(mc.ts)
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID) AS EarliestStatusTime
FROM rdb.Device d order by d.deviceID;


--Query the device ID of each device instance in the MeterSuper table of the tsdb library and the number of device models of the same device instance in the Device table of the rdb library:

SELECT ms.deviceID,
    (SELECT COUNT(dm.ModelID)
     FROM rdb.DeviceModel dm
     WHERE dm.ModelID = (
         SELECT d.modelID
         FROM rdb.Device d
         WHERE d.deviceID = ms.deviceID
     )) AS TypeCount
FROM tsdb.MeterSuper ms order by ms.deviceID;


--Query the device ID of each device instance in the Device table of the rdb library and the status average of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT AVG(mc.status)
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID) AS AverageStatus
FROM rdb.Device d order by d.deviceID;


--Get the device quantity and average current value of each device model, and sort them in descending order by device quantity:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT AVG(m.current) FROM tsdb.MeterSuper m WHERE m.modelID = dm.modelID) AS AvgCurrent
FROM rdb.DeviceModel dm
ORDER BY DeviceCount DESC, dm.ModelName;


--Get the earliest installation date and the corresponding maximum current for each device model, and sort in ascending order by the earliest installation date:

SELECT dm.ModelName,
       (SELECT MIN(d.installDate) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS EarliestInstallDate,
       (SELECT MAX(m.current) FROM tsdb.MeterSuper m WHERE m.modelID = dm.modelID) AS MaxCurrent
FROM rdb.DeviceModel dm
ORDER BY EarliestInstallDate ASC, dm.ModelName;


--Get the number of devices and the corresponding minimum temperature value for each device model, and sort them in descending order by the number of devices:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT MIN(t.temperature) FROM tsdb.TransformerSuper t WHERE t.modelID = dm.modelID) AS MinTemperature
FROM rdb.DeviceModel dm
ORDER BY DeviceCount DESC, dm.ModelName;


--Get the number of devices and the corresponding frequency average for each device model. If the frequency average is greater than 50, sort them in descending order according to the frequency average:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT AVG(m.frequency) FROM tsdb.MeterSuper m WHERE m.modelID = dm.modelID) AS AvgFrequency
FROM rdb.DeviceModel dm
WHERE (SELECT AVG(m.frequency) FROM tsdb.MeterSuper m WHERE m.modelID = dm.modelID) > 50
ORDER BY AvgFrequency DESC, dm.ModelName;


--Get the number of devices and the corresponding voltage sum of each device model, and sort them in descending order by voltage sum:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT SUM(m.voltage) FROM tsdb.MeterSuper m WHERE m.modelID = dm.modelID) AS TotalVoltage
FROM rdb.DeviceModel dm
ORDER BY TotalVoltage DESC, dm.ModelName;


--Get the location and corresponding current value of the device whose device model is 'electricity meter model 2', and sort them in ascending order by location name:

SELECT d.location, m.current AS Current
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
JOIN (
    SELECT deviceID, current,
           ROW_NUMBER() OVER (PARTITION BY deviceID ORDER BY ts DESC) AS rn
    FROM tsdb.MeterSuper
) m ON m.deviceID = d.deviceID AND m.rn = 1
WHERE dm.ModelName = '电表模型2'
ORDER BY d.location ASC;



--Get the model name and corresponding temperature value of the transformer device with the highest temperature:

SELECT dm.ModelName,
       (SELECT MAX(t.temperature) FROM tsdb.TransformerSuper t WHERE t.modelID = dm.modelID) AS MaxTemperature
FROM rdb.DeviceModel dm
WHERE EXISTS (
    SELECT 1
    FROM tsdb.TransformerSuper t
    WHERE t.modelID = dm.modelID
)
ORDER BY MaxTemperature DESC;


--Get the model name and corresponding current value of each device, and sort them in descending order by current value:

SELECT dm.ModelName,
       (SELECT m.current FROM tsdb.MeterSuper m WHERE m.deviceID = d.deviceID limit 1) AS Current
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
ORDER BY modelname,Current DESC;


--Get the location and corresponding status value of each device. The number of devices with status value 1 is greater than 2. Sort in descending order by location name:

SELECT d.location,
       (SELECT mc.status FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = d.deviceID) AS Status
FROM rdb.Device d
WHERE (SELECT COUNT(*) FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = d.deviceID AND mc.status = 1) > 2
ORDER BY d.location DESC;


--Get the model name and corresponding frequency average of each device, and sort them in descending order by frequency average:

SELECT dm.ModelName,
       (SELECT AVG(m.frequency) FROM tsdb.MeterSuper m WHERE m.deviceID = d.deviceID) AS AvgFrequency
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
ORDER BY AvgFrequency DESC, dm.ModelName;


--Scalar subquery in where

--Query the device ID in the Device table of the rdb library whose device installation date is earlier than the earliest status time of a device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE d.installDate < (
    SELECT MIN(mc.ts)
    FROM tsdb.MonitoringCenter mc
);


--Query the device IDs whose current in the MeterSuper table of the tsdb library is greater than the average current of a device instance in the Device table of the rdb library:

SELECT ms.deviceID
FROM tsdb.MeterSuper ms
WHERE ms.current > (
    SELECT AVG(ms2.current)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d
        WHERE d.modelID = 1 and d.deviceID=1
    )
) order by deviceid;


--Query the device ID of the device model in the Device table of the rdb library, which is the device model of a device instance in the MeterSuper table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE d.modelID = (
    SELECT ms.modelId
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d ORDER by d.deviceID limit 1
    ) ORDER by ms.modelID limit 1
);


--Query the device ID whose voltage in the MeterSuper table of the tsdb library is greater than the minimum voltage of a device instance in the Device table of the rdb library: Query the device ID whose voltage in the MeterSuper table of the tsdb library is greater than the minimum voltage of a device instance in the Device table of the rdb library:

SELECT ms.deviceID
FROM tsdb.MeterSuper ms
WHERE ms.voltage > (
    SELECT MIN(ms2.voltage)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d ORDER by d.deviceID limit 1
    )
)order by deviceid;


--Query the device ID in the Device table of the rdb library whose device installation date is equal to the earliest status time of a device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE d.installDate = (
    SELECT MIN(mc.ts)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d ORDER by d.deviceID limit 1
    )
);


--Query the device ID whose current in the MeterSuper table of the tsdb library is less than the maximum current of a device instance in the Device table of the rdb library:

SELECT ms.deviceID
FROM tsdb.MeterSuper ms
WHERE ms.current < (
    SELECT MAX(ms2.current)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d
        WHERE d.modelID = 1  ORDER by d.deviceID limit 1
    )
) order by deviceid;


--Query the device model whose device ID in the Device table of the rdb library is equal to the device ID of a device instance in the MeterSuper table of the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
WHERE dm.ModelID = (
    SELECT d.modelID
    FROM rdb.Device d
    WHERE d.deviceID = (
        SELECT ms.deviceID
        FROM
 tsdb.MeterSuper ms ORDER by ms.deviceID limit 1
    )
);


--Query the device ID whose current in the MeterSuper table of the tsdb library is equal to the average current of a device instance in the Device table of the rdb library:

SELECT ms.deviceID
FROM tsdb.MeterSuper ms
WHERE ms.current = (
    SELECT AVG(ms2.current)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d
        WHERE d.modelID = 1 ORDER by d.deviceID limit 1
    )
);


--Query the device model whose device ID in the Device table of the rdb library is equal to the device ID of a device instance in the MonitoringCenter table of the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
WHERE dm.ModelID = (
    SELECT d.modelID
    FROM rdb.Device d
    WHERE d.deviceID = (
        SELECT mc.deviceID
        FROM tsdb.MonitoringCenter mc ORDER by mc.deviceID limit 1
    )
);


--Query the device ID whose voltage in the MeterSuper table of the tsdb library is equal to the minimum voltage of a device instance in the Device table of the rdb library:

SELECT ms.deviceID
FROM tsdb.MeterSuper ms
WHERE ms.voltage = (
    SELECT MIN(ms2.voltage)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d ORDER by d.deviceID limit 1
    )
);


--Get the number of devices and the corresponding maximum current for each device model, where the maximum current is greater than 100, and sort in descending order by the number of devices:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT MAX(m.current) FROM tsdb.MeterSuper m WHERE m.deviceID = ANY(SELECT d.deviceID FROM rdb.Device d WHERE d.modelID = dm.modelID)) AS MaxCurrent
FROM rdb.DeviceModel dm
ORDER BY DeviceCount DESC, dm.ModelName;


--Get the number of devices and the corresponding average temperature of each device model, where the average temperature is less than 50, and sort in ascending order by the number of devices:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT AVG(t.temperature) FROM tsdb.TransformerSuper t WHERE t.deviceID = ANY(SELECT d.deviceID FROM rdb.Device d WHERE d.modelID = dm.modelID)) AS AvgTemperature
FROM rdb.DeviceModel dm
WHERE (SELECT AVG(t.temperature) FROM tsdb.TransformerSuper t WHERE t.deviceID = ANY(SELECT d.deviceID FROM rdb.Device d WHERE d.modelID = dm.modelID)) < 50
ORDER BY DeviceCount ASC, AvgTemperature;


--Get the number of devices and the corresponding minimum frequency for each device model. If the number of devices is greater than 3, sort them in descending order: Get the number of devices and the corresponding minimum frequency for each device model. If the number of devices is greater than 3, sort them in descending order:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT MIN(m.frequency) FROM tsdb.MeterSuper m WHERE m.deviceID = ANY(SELECT d.deviceID FROM rdb.Device d WHERE d.modelID = dm.modelID)) AS MinFrequency
FROM rdb.DeviceModel dm
WHERE (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) > 3
ORDER BY DeviceCount DESC, MinFrequency;


--Get the number of devices and the corresponding voltage sum for each device model, and sort them in ascending order by voltage sum:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT SUM(m.voltage) FROM tsdb.MeterSuper m WHERE m.deviceID = ANY(SELECT d.deviceID FROM rdb.Device d WHERE d.modelID = dm.modelID)) AS TotalVoltage
FROM rdb.DeviceModel dm
ORDER BY TotalVoltage ASC, dm.ModelName;


--Get the number of devices of each device model and the number of devices with the corresponding status 1, and sort them in descending order by the number of devices:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT COUNT(*) FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = ANY(SELECT d.deviceID FROM rdb.Device d WHERE d.modelID = dm.modelID) AND mc.status = 1) AS StatusCount
FROM rdb.DeviceModel dm
ORDER BY DeviceCount DESC, dm.ModelName;


--Scalar subquery in having

--Query the device models whose number of devices in the Device table of the rdb library is greater than the number of devices with a current greater than 100 in the MeterSuper table of the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
GROUP BY dm.TypeName
HAVING COUNT(*) > (
    SELECT COUNT(*)
    FROM tsdb.MeterSuper ms
    WHERE ms.current > 100
) order by TypeName;


--Query the device ID of each device instance in the MeterSuper table of the tsdb library, where the current is greater than the number of device IDs in the Device table of the rdb library:

SELECT ms.deviceID
FROM tsdb.MeterSuper ms
GROUP BY ms.deviceID
HAVING COUNT(*) > (
    SELECT COUNT(d.deviceID)
    FROM rdb.Device d
) order by ms.deviceID;


--Query the device ID and device model of each device instance in the Device table of the rdb library. The average current value of the device instance with the same device ID in the MeterSuper table of the tsdb library is greater than 100:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
GROUP BY d.deviceID, dm.TypeName
HAVING AVG(d.deviceID) > (
    SELECT AVG(ms.current)
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
);


--Query the device ID and current of each device instance in the MeterSuper table of the tsdb library, where the device ID exists in the Device table of the rdb library and the device model is 1:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
GROUP BY ms.deviceID, ms.current
HAVING ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.modelID = 1
) order by deviceid,current
;


--Query the device ID of each device instance in the Device table of the rdb library, where the device ID exists in the MonitoringCenter table of the tsdb library and the number of states is greater than 5:

SELECT d.deviceID
FROM rdb.Device d
GROUP BY d.deviceID
HAVING d.deviceID IN (
    SELECT mc.deviceID
    FROM tsdb.MonitoringCenter mc
    GROUP BY mc.deviceID
    HAVING COUNT(*) > 5
);


--Query the device ID and current of each device instance in the MeterSuper table of the tsdb library, where the device ID exists in the Device table of the rdb library and the device model is 2, and the current is greater than the maximum current of the device with device model 2 in the Device table of the rdb library:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.modelID = 2
)
GROUP BY ms.deviceID, ms.current
HAVING ms.current > (
    SELECT MAX(ms2.current)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID IN (
        SELECT d.deviceID
        FROM rdb.Device d
        WHERE d.modelID = 2
    )
);


--Query the device ID and device model of each device instance in the Device table of the rdb library, where the status average value of the device instance with the same device ID in the MonitoringCenter table of the tsdb library is less than -1:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
GROUP BY d.deviceID, dm.TypeName
HAVING AVG(d.deviceID) < (
    SELECT AVG(mc.status)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
);


--Query the device ID and voltage of each device instance in the MeterSuper table of the tsdb library, where the device ID exists in the Device table of the rdb library and the device model is 1, and the voltage is less than the minimum voltage of the device with device model 1 in the Device table of the rdb library:

SELECT ms.deviceID, ms.voltage
FROM tsdb.MeterSuper ms
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.modelID = 1
)
GROUP BY ms.deviceID, ms.voltage
HAVING ms.voltage < (
    SELECT MIN(ms2.voltage)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID IN (
        SELECT d.deviceID
        FROM rdb.Device d
        WHERE d.modelID = 1
    )
);


--Query the device ID and device model of each device instance in the Device table of the rdb library. The current average value of the device instance with the same device ID in the MeterSuper table of the tsdb library is greater than 50, and the device model is 2:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
GROUP BY d.deviceID, dm.TypeName,dm.modelID
HAVING AVG(d.deviceID) > 50
AND dm.ModelID = 2
AND d.deviceID IN (
    SELECT ms.deviceID
    FROM tsdb.MeterSuper ms
);


--Query the device ID and current of each device instance in the MeterSuper table of the tsdb library, where the device ID exists in the Device table of the rdb library and the device model is 1, and the current is less than the average current of the device with device model 1 in the Device table of the rdb library:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.modelID = 1
)
GROUP BY ms.deviceID, ms.current
HAVING ms.current < (
    SELECT AVG(ms2.current)
    FROM tsdb.MeterSuper ms2
    WHERE ms2.deviceID IN (
        SELECT d.deviceID
        FROM rdb.Device d
        WHERE d.modelID = 1
    )
) order by deviceid,current;


--Get the number of devices and the corresponding maximum current for each device model. If the number of devices is greater than 3 and the maximum current is greater than 100, sort them in descending order by the number of devices:

SELECT dm.ModelName,
       COUNT(*) AS DeviceCount,
       MAX(m.current) AS MaxCurrent
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.modelID = d.modelID
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
GROUP BY dm.ModelName
HAVING COUNT(*) > 3 AND MAX(m.current) > 100
ORDER BY DeviceCount DESC;


--Get the number of devices and the corresponding average temperature of each device model. If the number of devices is greater than 2 and the average temperature is less than 50, sort them in ascending order by the number of devices:

SELECT dm.ModelName,
       COUNT(*) AS DeviceCount,
       AVG(t.temperature) AS AvgTemperature
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.modelID = d.modelID
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
GROUP BY dm.ModelName
HAVING COUNT(*) > 2 AND AVG(t.temperature) < 50
ORDER BY DeviceCount ASC, dm.ModelName;


--Get the number of devices and the corresponding minimum frequency for each device model, where the number of devices is greater than 1 and the minimum frequency is greater than 50, and sort in descending order by the number of devices:

SELECT dm.ModelName,
       COUNT(*) AS DeviceCount,
       MIN(m.frequency) AS MinFrequency
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.modelID = d.modelID
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
GROUP BY dm.ModelName
HAVING COUNT(*) > 1 AND MIN(m.frequency) > 50
ORDER BY DeviceCount DESC;


--Get the number of devices and the corresponding voltage sum of each device model. If the number of devices is greater than 3, sort them in ascending order by the voltage sum:

SELECT dm.ModelName,
       COUNT(*) AS DeviceCount,
       SUM(m.voltage) AS TotalVoltage
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.modelID = d.modelID
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
GROUP BY dm.ModelName
HAVING COUNT(*) > 3
ORDER BY TotalVoltage ASC;


--Get the number of devices of each device model and the number of devices with the corresponding status of 1. If the number of devices is greater than 2, sort them in descending order by the number of devices:

SELECT dm.ModelName,
COUNT(1) AS DeviceCount,
(SELECT COUNT(1) FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = d.deviceID AND mc.status = 1) AS StatusCount
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.modelID = d.modelID
GROUP BY dm.ModelName, d.deviceID
HAVING COUNT(*) > 2
ORDER BY DeviceCount DESC;

--Scalar subquery in having

--Get the location and corresponding current value of the device whose device model is 'electricity meter model 1', and sort them in ascending order by location name:

SELECT d.location,
       m.current AS Current
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
WHERE d.modelID = (SELECT modelID FROM rdb.DeviceModel WHERE ModelName = '电表模型1')
ORDER BY d.location ASC;


--Get the model names and corresponding temperature values of devices whose status is 0 and current is less than 100, and sort them in descending order by temperature value:

SELECT dm.ModelName,
t.temperature AS Temperature
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
WHERE d.deviceID IN (SELECT mc.deviceID FROM tsdb.MonitoringCenter mc WHERE mc.status = 0)
AND (SELECT MAX(m.current) FROM tsdb.MeterSuper m WHERE m.deviceID = d.deviceID) < 100
ORDER BY Temperature DESC;



--Get the model name and corresponding current value of each device, where the current value is greater than the average current value, and sort in descending order by current value:

SELECT dm.ModelName,
       m.current AS Current
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
WHERE m.current > (SELECT AVG(current) FROM tsdb.MeterSuper)
ORDER BY Current DESC;


--Get the location and corresponding status value of each device, where the number of devices is greater than 1, and sort in descending order by location name:

SELECT d.location,
       mc.status AS Status
FROM rdb.Device d
         JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE EXISTS (
              SELECT 1
              FROM rdb.Device d2
              WHERE d2.location = d.location
              HAVING COUNT(*) > 1
          )
ORDER BY d.location DESC,status;


--Get the model name and corresponding frequency average of each device, and sort them in descending order by frequency average:

SELECT dm.ModelName,
       m.frequency AS AvgFrequency
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
GROUP BY dm.ModelName, m.frequency
ORDER BY AvgFrequency DESC;


--Scalar subquery in order by


--Query the device ID and device model in the Device table of the rdb library, and sort in descending order according to the average current value of the same device ID in the MeterSuper table of the tsdb library:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY (
    SELECT AVG(ms.current)
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
) DESC,d.deviceID;


--Query the device ID and current in the MeterSuper table of the tsdb library, and sort in ascending order according to the device models with the same device ID in the Device table of the rdb library:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
ORDER BY (
    SELECT dm.TypeID
    FROM rdb.DeviceModel dm
    WHERE dm.ModelID = (
        SELECT d.modelID
        FROM rdb.Device d
        WHERE d.deviceID = ms.deviceID
    )
) , deviceid, current ASC;


--Query the device ID and device model in the Device table of the rdb library, and sort in descending order according to the difference between the maximum and minimum current values of the same device ID in the MeterSuper table of the tsdb library:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY (
    SELECT MAX(ms.current) - MIN(ms.current)
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
) DESC,d.deviceID;


--Query the device ID and current in the MeterSuper table of the tsdb library, and sort in descending order according to the installation date of the same device ID in the Device table of the rdb library:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
ORDER BY (
    SELECT d.installDate
    FROM rdb.Device d
    WHERE d.deviceID = ms.deviceID
) DESC,deviceid,current;


--Query the device ID and device model in the Device table of the rdb library, and sort in ascending order according to the number of states with the same device ID in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY (
    SELECT COUNT(mc.status)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
) ASC, d.deviceID;


--Query the device ID and current in the MeterSuper table of the tsdb library, and sort in ascending order according to the device model and installation date of the same device ID in the Device table of the rdb library:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
ORDER BY (
    SELECT dm.TypeID
    FROM rdb.Device d
    JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
    WHERE d.deviceID = ms.deviceID
) ASC,deviceid,current;


--Query the device ID and device model in the Device table of the rdb library, and sort in descending order according to the sum of the average current and average frequency of the same device ID in the MeterSuper table of the tsdb library:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY (
    SELECT AVG(ms.current) + AVG(ms.frequency)
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
) DESC,d.deviceID;


--Query the device ID and current in the MeterSuper table of the tsdb library, and sort in descending order according to the device model and installation date of the same device ID in the Device table of the rdb library:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
ORDER BY (
    SELECT d.installDate
    FROM rdb.Device d
    JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
    WHERE d.deviceID = ms.deviceID
) DESC,deviceid,current;


--Query the device ID and device model in the Device table of the rdb library, and sort in ascending order according to the earliest status time of the same device ID in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY (
    SELECT MIN(mc.ts)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
) ASC, d.deviceID;


--Query the device ID and current in the MeterSuper table of the tsdb library, and sort in ascending order according to the device model and installation date of the same device ID in the Device table of the rdb library:

SELECT ms.deviceID, ms.current
FROM tsdb.MeterSuper ms
ORDER BY (
    SELECT  d.installDate
    FROM rdb.Device d
    JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
    WHERE d.deviceID = ms.deviceID
) ASC,deviceid,current;


--The projection subquery must be a single column.


--Query the device ID in the Device table of the rdb library, where there is a record with the same device ID in the MeterSuper table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE d.deviceID IN (
    SELECT ms.deviceID
    FROM tsdb.MeterSuper ms
) order by deviceid;


--Query the current in the MeterSuper table of the tsdb library, where the record with device ID equal to 1 in the Device table of the rdb library exists:

SELECT ms.current
FROM tsdb.MeterSuper ms
WHERE ms.deviceID = (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.deviceID = 1
);


--Query the device ID in the Device table of the rdb library, where the records with voltage greater than 100 in the MeterSuper table of the tsdb library exist:

SELECT d.deviceID
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
    AND ms.voltage > 100
) order by deviceid;


--Query the voltage in the MeterSuper table of the tsdb library, where the record with device model 2 in the Device table of the rdb library exists:

SELECT ms.voltage
FROM tsdb.MeterSuper ms
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.modelID = 2
) order by voltage;


--Query the device ID in the Device table of the rdb library, where the number of records with states greater than 10 exists in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    HAVING COUNT(*) > 10
);


--Query the current in the MeterSuper table of the tsdb library, where the records with device IDs between 1 and 5 in the Device table of the rdb library exist:

SELECT ms.current
FROM tsdb.MeterSuper ms
WHERE ms.deviceID BETWEEN (
    SELECT MIN(d.deviceID)
    FROM rdb.Device d
) AND (
    SELECT MAX(d.deviceID)
    FROM rdb.Device d
) order by current;

-- ZDP-21582
SELECT ms.current
FROM tsdb.MeterSuper ms
WHERE ms.deviceID BETWEEN 1 AND 5 order by current;

--Query the device ID in the Device table of the rdb library, where the record with negative current in the MeterSuper table of the tsdb library exists:

SELECT d.deviceID
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
    AND ms.current < 0
);


--Query the current in the MeterSuper table of the tsdb library, where the record whose device ID in the Device table of the rdb library exists in the MeterSuper table of the tsdb library exists:

SELECT ms.current
FROM tsdb.MeterSuper ms
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE EXISTS (
        SELECT 1
        FROM tsdb.MeterSuper ms2
        WHERE ms2.deviceID = d.deviceID
    )
) order by current;


--Query the device ID in the Device table of the rdb library, and the record with status 0 in the MonitoringCenter table of the tsdb library

SELECT d.deviceID
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    AND mc.status = 0
)order by deviceID;


--Query the voltage in the MeterSuper table of the tsdb library, where the device model in the Device table of the rdb library exists in the record in the MeterSuper table of the tsdb library:

SELECT ms.voltage
FROM tsdb.MeterSuper ms
WHERE ms.deviceID IN (
    SELECT d.deviceID
    FROM rdb.Device d
    WHERE d.modelID IN (
        SELECT DISTINCT ms2.modelID
        FROM tsdb.MeterSuper ms2
    )
) order by voltage;




--The projection subquery must have multiple columns.


--Query the device ID and device model in the Device table of the rdb library, where there is a record with the same device ID in the MeterSuper table of the tsdb library, and return the current and voltage of the corresponding device ID in the tsdb library:

SELECT d.deviceID, dm.TypeName, ms.current, ms.voltage
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
JOIN tsdb.MeterSuper ms ON d.deviceID = ms.deviceID order by deviceid,current;


--Query the device ID and current in the MeterSuper table of the tsdb library. The record with the device ID equal to 1 in the Device table of the rdb library exists. At the same time, return the device model and installation date of the corresponding device ID in the rdb library:

SELECT ms.deviceID, ms.current, dm.TypeName, d.installDate
FROM tsdb.MeterSuper ms
JOIN rdb.Device d ON ms.deviceID = d.deviceID
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
WHERE d.deviceID = 1;


--Query the device ID and device model in the Device table of the rdb library, where the records with voltage greater than 100 in the MeterSuper table of the tsdb library exist, and return the current and frequency of the corresponding device ID in the tsdb library:

SELECT d.deviceID, dm.TypeName, ms.current, ms.frequency
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
JOIN tsdb.MeterSuper ms ON d.deviceID = ms.deviceID
WHERE ms.voltage > 100 order by deviceid,current;


--Query the device ID and current in the MeterSuper table of the tsdb library. The record with device model 2 in the Device table of the rdb library exists. At the same time, return the device model and installation date of the corresponding device ID in the rdb library:

SELECT ms.deviceID, ms.current, dm.TypeName, d.installDate
FROM tsdb.MeterSuper ms
JOIN rdb.Device d ON ms.deviceID = d.deviceID
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
WHERE d.modelID = 2 order by deviceid,current;


--Query the device ID and device model in the Device table of the rdb library, where the records with a status quantity greater than 10 exist in the MonitoringCenter table of the tsdb library, and return the status average and maximum values ​​of the corresponding device ID in the tsdb library:

SELECT d.deviceID, dm.TypeName, AVG(mc.status), MAX(mc.status)
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
GROUP BY d.deviceID, dm.TypeName
HAVING COUNT(mc.status) > 10;


--Query the device ID and current in the MeterSuper table of the tsdb library, where the records with device IDs between 1 and 5 in the Device table of the rdb library exist, and return the device model and installation date of the corresponding device ID in the rdb library:

SELECT ms.deviceID, ms.current, dm.TypeName, d.installDate
FROM tsdb.MeterSuper ms
JOIN rdb.Device d ON ms.deviceID = d.deviceID
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
WHERE d.deviceID BETWEEN 1 AND 5 order by deviceid,current;


--Query the device ID and device model in the Device table of the rdb library. The records with negative current in the MeterSuper table of the tsdb library are found. At the same time, the current and voltage of the corresponding device ID in the tsdb library are returned:

SELECT d.deviceID, dm.TypeName, ms.current, ms.voltage
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
JOIN tsdb.MeterSuper ms ON d.deviceID = ms.deviceID
WHERE ms.current < 0;


--Query the current in the MeterSuper table of the tsdb library, where the record with the device ID in the Device table of the rdb library exists in the MeterSuper table of the tsdb library, and return the device model and installation date of the corresponding device ID in the rdb library:

SELECT ms.current, dm.TypeName, d.installDate
FROM tsdb.MeterSuper ms
JOIN rdb.Device d ON ms.deviceID = d.deviceID
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
WHERE d.deviceID IN (
    SELECT deviceID
    FROM tsdb.MeterSuper
) order by current;


--Query the device ID and device model in the Device table of the rdb library, where the records with a status of 0 exist in the MonitoringCenter table of the tsdb library, and return the average and minimum status values ​​of the corresponding device ID in the tsdb library:

SELECT d.deviceID, dm.TypeName, AVG(mc.status), MIN(mc.status)
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE mc.status = 0
GROUP BY d.deviceID, dm.TypeName order by d.deviceID;


--Query the voltage in the MeterSuper table of the tsdb library, where the device model in the Device table of the rdb library exists in the MeterSuper table of the tsdb library, and return the device model and installation date of the corresponding device ID in the rdb library:

SELECT ms.voltage, dm.TypeName, d.installDate
FROM tsdb.MeterSuper ms
JOIN rdb.Device d ON ms.deviceID = d.deviceID
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
WHERE d.modelID IN (
    SELECT DISTINCT modelID
    FROM tsdb.MeterSuper
) order by voltage;




--union query SQL statement

--Query the device ID and device model in the Device table of the rdb library, and the combined result of the device ID and current in the MeterSuper table of the tsdb library:

--Query the device ID and timestamp in the Device table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT deviceID, NULL AS ts
FROM rdb.Device
UNION
SELECT NULL AS deviceID, ts
FROM tsdb.MonitoringCenter order by deviceid, ts;


--Query the model ID and current value in the DeviceModel table of the rdb library and the MeterSuper table of the tsdb library:

SELECT ModelID, NULL AS current
FROM rdb.DeviceModel
UNION
SELECT NULL AS ModelID, current
FROM tsdb.MeterSuper order by modelid, current;


--Query the device ID and status in the Device table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT deviceID, NULL AS status
FROM rdb.Device
UNION
SELECT NULL AS deviceID, status
FROM tsdb.MonitoringCenter order by deviceID,status;


--Query the model name and frequency in the DeviceModel table of the rdb library and the MeterSuper table of the tsdb library:

SELECT ModelName, NULL AS frequency
FROM rdb.DeviceModel
UNION
SELECT NULL AS ModelName, frequency
FROM tsdb.MeterSuper order by ModelName, frequency;


--Query the device ID and temperature in the Device table of the rdb library and the TransformerSuper table of the tsdb library:

SELECT deviceID, NULL AS temperature
FROM rdb.Device
UNION
SELECT NULL AS deviceID, temperature
FROM tsdb.TransformerSuper order by deviceID,temperature;


--Query the model type and status in the DeviceModel table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT TypeName, NULL AS status
FROM rdb.DeviceModel
UNION
SELECT NULL AS TypeName, status
FROM tsdb.MonitoringCenter order by TypeName,status;


--Query the device installation date and voltage in the Device table of the rdb library and the MeterSuper table of the tsdb library:

SELECT installDate, NULL AS voltage
FROM rdb.Device
UNION
SELECT NULL AS installDate, voltage
FROM tsdb.MeterSuper  order by installDate, voltage;


--Query the model type and frequency in the DeviceModel table of the rdb library and the TransformerSuper table of the tsdb library:

SELECT TypeName, NULL AS frequency
FROM rdb.DeviceModel
UNION
SELECT NULL AS TypeName, frequency
FROM tsdb.TransformerSuper order by TypeName,frequency;


--Query the device location and timestamp in the Device table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT location, NULL AS ts
FROM rdb.Device
UNION
SELECT NULL AS location, ts
FROM tsdb.MonitoringCenter order by location, ts;


--Query the model name and current value in the DeviceModel table of the rdb library and the MeterSuper table of the tsdb library:

SELECT ModelName, NULL AS current
FROM rdb.DeviceModel
UNION
SELECT NULL AS ModelName, current
FROM tsdb.MeterSuper order by ModelName,current;


--Cross-database union all query SQL statement


--Query the Device table in the rdb library and the MonitoringCenter table in the tsdb library, and sort them in ascending order by device ID:

SELECT deviceID, installDate, location, NULL AS ts, NULL AS status
FROM rdb.Device
UNION ALL
SELECT NULL AS deviceID, NULL AS installDate, NULL AS location, ts, status
FROM tsdb.MonitoringCenter
ORDER BY deviceID ASC,ts;


--Query the DeviceModel table in the rdb library and the MonitoringCenter table in the tsdb library, and sort them in ascending order by model ID:

SELECT ModelID, TypeID, ModelName, TypeName, NULL AS ts, NULL AS status
FROM rdb.DeviceModel
UNION ALL
SELECT NULL AS ModelID, NULL AS TypeID, NULL AS ModelName, NULL AS TypeName, ts, status
FROM tsdb.MonitoringCenter
ORDER BY ModelID ASC,ts;


--Query the Device table in the rdb library and the MeterSuper table in the tsdb library, and sort them in ascending order by device ID:

SELECT deviceID, installDate, location, NULL AS ts, NULL AS current, NULL AS voltage, NULL AS frequency
FROM rdb.Device
UNION ALL
SELECT NULL AS deviceID, NULL AS installDate, NULL AS location, ts, current, voltage, frequency
FROM tsdb.MeterSuper
ORDER BY deviceID ASC,ts;


--Query the DeviceModel table in the rdb library and the MeterSuper table in the tsdb library, and sort them in ascending order by model ID:

SELECT ModelID, TypeID, ModelName, TypeName, NULL AS ts, NULL AS current, NULL AS voltage, NULL AS frequency
FROM rdb.DeviceModel
UNION ALL
SELECT NULL AS ModelID, NULL AS TypeID, NULL AS ModelName, NULL AS TypeName, ts, current, voltage, frequency
FROM tsdb.MeterSuper
ORDER BY ModelID ASC,ts;


--Query the Device table in the rdb library and the TransformerSuper table in the tsdb library, and sort them in ascending order by device ID:

SELECT deviceID, installDate, location, NULL AS ts, NULL AS current, NULL AS voltage, NULL AS frequency, NULL AS temperature
FROM rdb.Device
UNION ALL
SELECT NULL AS deviceID, NULL AS installDate, NULL AS location, ts, current, voltage, frequency, temperature
FROM tsdb.TransformerSuper
ORDER BY deviceID ASC,ts;


--Query the DeviceModel table in the rdb library and the TransformerSuper table in the tsdb library, and sort them in ascending order by model ID:

SELECT ModelID, TypeID, ModelName, TypeName, NULL AS ts, NULL AS current, NULL AS voltage, NULL AS frequency, NULL AS temperature
FROM rdb.DeviceModel
UNION ALL
SELECT NULL AS ModelID, NULL AS TypeID, NULL AS ModelName, NULL AS TypeName, ts, current, voltage, frequency, temperature
FROM tsdb.TransformerSuper
ORDER BY ModelID ASC,ts;


--Query the Device table of the rdb library and the MonitoringCenter table of the tsdb library, and sort them in descending order by installation date:

SELECT deviceID, installDate, location, NULL AS ts, NULL AS status
FROM rdb.Device
UNION ALL
SELECT NULL AS deviceID, NULL AS installDate, NULL AS location, ts, status
FROM tsdb.MonitoringCenter
ORDER BY installDate DESC,deviceid,ts;


--Query the DeviceModel table of the rdb library and the MonitoringCenter table of the tsdb library, and sort them in descending order by type ID:

SELECT ModelID, TypeID, ModelName, TypeName, NULL AS ts, NULL AS status
FROM rdb.DeviceModel
UNION ALL
SELECT NULL AS ModelID, NULL AS TypeID, NULL AS ModelName, NULL AS TypeName, ts, status
FROM tsdb.MonitoringCenter
ORDER BY ModelID DESC,ts;


--Query the Device table in the rdb library and the MeterSuper table in the tsdb library, and sort them in descending order by device location:

SELECT deviceID, installDate, location, NULL AS ts, NULL AS current, NULL AS voltage, NULL AS frequency
FROM rdb.Device
UNION ALL
SELECT NULL AS deviceID, NULL AS installDate, NULL AS location, ts, current, voltage, frequency
FROM tsdb.MeterSuper
ORDER BY location DESC,deviceid,ts;


--Query the DeviceModel table in the rdb library and the MeterSuper table in the tsdb library, and sort them in descending order by type name:

SELECT ModelID, TypeID, ModelName, TypeName, NULL AS ts, NULL AS current, NULL AS voltage, NULL AS frequency
FROM rdb.DeviceModel
UNION ALL
SELECT NULL AS ModelID, NULL AS TypeID, NULL AS ModelName, NULL AS TypeName, ts, current, voltage, frequency
FROM tsdb.MeterSuper
ORDER BY TypeName DESC,modelid,ts;


--Cross-database intersect query SQL statement


--Query the device IDs that have records in the `Device` table in the `rdb` library and also have records in the `MeterSuper` table in the `tsdb` library:

SELECT deviceID
FROM rdb.Device
INTERSECT
SELECT deviceID
FROM tsdb.MeterSuper order by deviceid;


--Query the device IDs whose installation date is '2023-07-01' in the `Device` table of the `rdb` library and whose current is greater than 0 in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-01'
INTERSECT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current > 0 order by deviceid;


--Query the device IDs that are located in the 'area1' area in the 'Device' table of the 'rdb' library and have a frequency less than 50 in the 'TransformerSuper' table of the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area1'
INTERSECT
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE frequency < 50 order by deviceid;


--Query the device IDs whose device model is 1 in the `Device` table of the `rdb` library and whose voltage is greater than 200 in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 1
INTERSECT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE voltage > 200 order by deviceid;


--Query the device IDs in the 'area2' area of ​​the 'Device' table in the 'rdb' library and in the 'MeterSuper' table in the 'tsdb' library whose voltage is greater than 100 and whose frequency is less than 60:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area2'
INTERSECT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE voltage > 100 AND frequency < 60 order by deviceid;


--Query the device model 2 in the `Device` table of the `rdb` library, and the device ID with a current greater than 10 and a temperature greater than 50 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 2
INTERSECT
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE current > 10 AND temperature > 50;


--Query the device IDs whose installation date is '2023-07-04' in the 'Device' table of the 'rdb' library and whose current and voltage are greater than 0 in the 'MeterSuper' table of the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-04'
INTERSECT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current > 0 AND voltage > 0 order by deviceid;


--Query the device model in the `Device` table of the `rdb` library. The device ID is 4. At the same time, in the `TransformerSuper` table of the `tsdb` library, the current is greater than 5 and the temperature is less than 40.
SELECT deviceID
FROM rdb.Device
WHERE modelID = 4
INTERSECT
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE current > 5 AND temperature < 40;


--Query the device IDs that are located in the 'area3' area in the 'Device' table in the 'rdb' library and have a current less than 50 in the 'MeterSuper' table in the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area3'
INTERSECT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current < 50 order by deviceid;


--Query the device IDs whose installation date is '2023-07-02' in the 'Device' table of the 'rdb' library and whose frequency is greater than 50 in the 'TransformerSuper' table of the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-02'
INTERSECT
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE frequency > 50 order by deviceid;

-- 1. Query the deviceID that appears in both rdb.Device and tsdb.MonitoringCenter
SELECT deviceID FROM rdb.Device
INTERSECT
SELECT deviceID FROM tsdb.MonitoringCenter
ORDER BY deviceID;

-- 2. Query modelID that appears in both rdb.DeviceModel and tsdb.MeterSuper
SELECT modelID FROM rdb.DeviceModel
INTERSECT
SELECT modelID FROM tsdb.MeterSuper
ORDER BY modelID DESC;

-- 3. Query modelID that appears in both rdb.DeviceModel and tsdb.TransformerSuper
SELECT modelID FROM rdb.DeviceModel
INTERSECT
SELECT modelId FROM tsdb.TransformerSuper
ORDER BY modelID DESC;

-- 4. Query the deviceID that appears in both rdb.Device and tsdb.MeterSuper
SELECT deviceID FROM rdb.Device
INTERSECT
SELECT deviceID FROM tsdb.MeterSuper
ORDER BY deviceID;

-- 5. Query deviceID that appears in both rdb.Device and tsdb.TransformerSuper
SELECT deviceID FROM rdb.Device
INTERSECT
SELECT deviceID FROM tsdb.TransformerSuper
ORDER BY deviceID DESC;

-- 6. Query deviceID that appears in both tsdb.MeterSuper and tsdb.TransformerSuper
SELECT deviceID FROM tsdb.MeterSuper
INTERSECT
SELECT deviceID FROM tsdb.TransformerSuper
ORDER BY deviceID;

-- 7. Query modelIDs that appear in both tsdb.MeterSuper and tsdb.TransformerSuper
SELECT modelID FROM tsdb.MeterSuper
INTERSECT
SELECT modelId FROM tsdb.TransformerSuper
ORDER BY modelID DESC;

-- 8. Query deviceID that appears in both tsdb.MeterSuper and tsdb.MonitoringCenter
SELECT deviceID FROM tsdb.MeterSuper
INTERSECT
SELECT deviceID FROM tsdb.MonitoringCenter
ORDER BY deviceID DESC;

-- 9. Query deviceID that appears in both tsdb.TransformerSuper and tsdb.MonitoringCenter
SELECT deviceID FROM tsdb.TransformerSuper
INTERSECT
SELECT deviceID FROM tsdb.MonitoringCenter
ORDER BY deviceID;

-- 10. Query modelID that appears in rdb.DeviceModel, tsdb.MeterSuper and tsdb.TransformerSuper
SELECT modelID FROM rdb.DeviceModel
INTERSECT
SELECT modelID FROM tsdb.MeterSuper
INTERSECT
SELECT modelId FROM tsdb.TransformerSuper
ORDER BY modelID;



--Cross-database intersect all query SQL statement

--Query the device IDs (including duplicate IDs) that have records in the `Device` table in the `rdb` library and also have records in the `MeterSuper` table in the `tsdb` library:

SELECT deviceID
FROM rdb.Device
INTERSECT ALL
SELECT deviceID
FROM tsdb.MeterSuper order by deviceid;


--Query the device IDs (including duplicate IDs) whose installation date is '2023-07-01' in the `Device` table of the `rdb` library and whose current is greater than 0 in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-01'
INTERSECT ALL
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current > 0  order by deviceid;


--Query the device IDs (including duplicate IDs) that are located in the 'area1' area in the 'Device' table of the 'rdb' library and have a frequency less than 50 in the 'TransformerSuper' table of the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area1'
INTERSECT ALL
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE frequency < 50  order by deviceid;


--Query the device IDs (including duplicate IDs) whose device model is 1 in the `Device` table of the `rdb` library and whose voltage is greater than 200 in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 1
INTERSECT ALL
SELECT deviceID
FROM tsdb.MeterSuper
WHERE voltage > 200 order by deviceid;


--Query the device IDs (including duplicate IDs) in the 'area2' area of ​​the 'Device' table in the 'rdb' library and in the 'MeterSuper' table in the 'tsdb' library with a voltage greater than 100 and a frequency less than 60:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area2'
INTERSECT ALL
SELECT deviceID
FROM tsdb.MeterSuper
WHERE voltage > 100 AND frequency < 60 order by deviceid;


--Query the device model 2 in the `Device` table of the `rdb` library, and the device IDs (including duplicate IDs) with current greater than 10 and temperature greater than 50 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 2
INTERSECT ALL
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE current > 10 AND temperature > 50 order by deviceid;


--Query the device IDs (including duplicate IDs) whose installation date is '2023-07-04' in the `Device` table of the `rdb` library and whose current and voltage are greater than 0 in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-04'
INTERSECT ALL
SELECT deviceID
FROM

 tsdb.MeterSuper
WHERE current > 0 AND voltage > 0 order by deviceid;


--Query the device IDs (including duplicate IDs) whose device model is 4 in the `Device` table of the `rdb` library and whose current is greater than 5 and temperature is less than 40 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 4
INTERSECT ALL
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE current > 5 AND temperature < 40 order by deviceid;


--Query the device IDs (including duplicate IDs) in the 'area3' area of ​​the 'Device' table in the 'rdb' library and whose current is less than 50 in the 'MeterSuper' table in the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area3'
INTERSECT ALL
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current < 50  order by deviceid;


--Query the device IDs (including duplicate IDs) whose installation date is '2023-07-02' in the `Device` table of the `rdb` library and whose frequency is greater than 50 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-02'
INTERSECT ALL
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE frequency > 50  order by deviceid;



--Cross-database except query SQL statement

--Query the device IDs that have records in the `Device` table of the `rdb` library but no records in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
EXCEPT
SELECT deviceID
FROM tsdb.MeterSuper  order by deviceid;


--Query the device IDs whose installation date is '2023-07-01' in the `Device` table of the `rdb` library, but which have no records in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-01'
EXCEPT
SELECT deviceID
FROM tsdb.MeterSuper  order by deviceid;


--Query the device ID that is located in the 'area1' area in the 'Device' table of the 'rdb' library but has no record in the 'TransformerSuper' table of the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area1'
EXCEPT
SELECT deviceID
FROM tsdb.TransformerSuper  order by deviceid;


--Query the device ID whose device model is 1 in the `Device` table of the `rdb` library but is not recorded in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 1
EXCEPT
SELECT deviceID
FROM tsdb.MeterSuper  order by deviceid;


--Query the device IDs in the 'area2' area of ​​the 'Device' table in the 'rdb' library and whose voltage is less than 100 in the 'MeterSuper' table in the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area2'
EXCEPT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE voltage < 100  order by deviceid;


--Query the device ID whose device model is 2 in the `Device` table of the `rdb` library but is not recorded in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 2
EXCEPT
SELECT deviceID
FROM tsdb.TransformerSuper  order by deviceid;


--Query the device IDs whose installation date is '2023-07-04' in the `Device` table of the `rdb` library, but whose current or voltage is less than or equal to 0 in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-04'
EXCEPT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current <= 0 OR voltage <= 0  order by deviceid;


--Query the device IDs whose device model is 4 in the `Device` table of the `rdb` library, but whose current is less than or equal to 5 or whose temperature is greater than or equal to 40 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 4
EXCEPT
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE current <= 5 OR temperature >= 40 order by deviceid;


--Query the device IDs that are located in the 'area3' area in the 'Device' table of the 'rdb' library and have a current greater than or equal to 50 in the 'MeterSuper' table of the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area3'
EXCEPT
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current >= 50  order by deviceid;


--Query the device IDs whose installation date is '2023-07-02' in the `Device` table of the `rdb` library, but whose frequency is less than or equal to 50 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-02'
EXCEPT
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE frequency <= 50  order by deviceid;



--Cross-library except all

--Query the device IDs (including duplicate IDs) that have records in the `Device` table of the `rdb` library and no records in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
EXCEPT ALL
SELECT deviceID
FROM tsdb.MeterSuper  order by deviceid;


--Query the device IDs (including duplicate IDs) whose installation date is '2023-07-01' in the `Device` table of the `rdb` library but which have no records in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-01'
EXCEPT ALL
SELECT deviceID
FROM tsdb.MeterSuper  order by deviceid;


--Query the device IDs (including duplicate IDs) that are located in the 'area1' area in the 'Device' table in the 'rdb' library but have no records in the 'TransformerSuper' table in the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area1'
EXCEPT ALL
SELECT deviceID
FROM tsdb.TransformerSuper order by deviceid;


--Query the device IDs (including duplicate IDs) whose device model is 1 in the `Device` table of the `rdb` library but which have no records in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 1
EXCEPT ALL
SELECT deviceID
FROM tsdb.MeterSuper order by deviceid;


--Query the device IDs (including duplicate IDs) in the 'area2' area of the 'Device' table of the 'rdb' library and whose voltage is less than 100 in the 'MeterSuper' table of the 'tsdb' library:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area2'
EXCEPT ALL
SELECT deviceID
FROM tsdb.MeterSuper
WHERE voltage < 100  order by deviceid;


--Query the device IDs (including duplicate IDs) whose device model is 2 in the `Device` table of the `rdb` library but which have no records in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 2
EXCEPT ALL
SELECT deviceID
FROM tsdb.TransformerSuper order by deviceid;


--Query the device IDs (including duplicate IDs) whose installation date is '2023-07-04' in the `Device` table of the `rdb` library, but whose current or voltage is less than or equal to 0 in the `MeterSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-04'
EXCEPT ALL
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current <= 0 OR voltage <= 0  order by deviceid;


--Query the device IDs (including duplicate IDs) whose device model is 4 in the `Device` table of the `rdb` library, but whose current is less than or equal to 5 or whose temperature is greater than or equal to 40 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE modelID = 4
EXCEPT ALL
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE current <= 5 OR temperature >= 40 order by deviceid;


--Query the device IDs (including duplicate IDs) in the 'area3' area of ​​the 'Device' table in the 'rdb' library and in the 'MeterSuper' table in the 'tsdb' library whose current is greater than or equal to 50:

SELECT deviceID
FROM rdb.Device
WHERE location = 'area3'
EXCEPT ALL
SELECT deviceID
FROM tsdb.MeterSuper
WHERE current >= 50 order by deviceid;


--Query the device IDs (including duplicate IDs) whose installation date is '2023-07-02' in the `Device` table of the `rdb` library but whose frequency is less than or equal to 50 in the `TransformerSuper` table of the `tsdb` library:

SELECT deviceID
FROM rdb.Device
WHERE installDate = '2023-07-02'
EXCEPT ALL
SELECT deviceID
FROM tsdb.TransformerSuper
WHERE frequency <= 50  order by deviceid;




--Multi-level nested subqueries



--Query the type and location of devices whose latest status is normal (0):

SELECT DISTINCT TypeName, location
FROM (
    SELECT modelID, location 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID, status
            FROM tsdb.MonitoringCenter
            WHERE ts IN (
                SELECT MAX(ts) 
                FROM tsdb.MonitoringCenter 
                WHERE status = 0
            )
        ) AS SubSubQuery1
    )
) AS SubQuery1
JOIN rdb.DeviceModel ON SubQuery1.modelID = rdb.DeviceModel.modelID
ORDER BY TypeName, location;


--Query the information and status of all devices whose current and voltage exceed the threshold:

SELECT SubQuery2.deviceID, ModelName, TypeName, status
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID 
        FROM (
            SELECT deviceID 
            FROM tsdb.MeterSuper
            WHERE current > '10' AND voltage > '220'
        ) AS SubSubQuery2
    )
) AS SubQuery2
JOIN rdb.DeviceModel ON SubQuery2.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MonitoringCenter ON SubQuery2.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC, SubQuery2.deviceID;


--Query the information of the device with the earliest installation date and its latest status:

SELECT SubQuery3.deviceID, ModelName, TypeName, status
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE installDate IN (
        SELECT MIN(installDate) 
        FROM (
            SELECT installDate 
            FROM rdb.Device
        ) AS SubSubQuery3
    )
) AS SubQuery3
JOIN rdb.DeviceModel ON SubQuery3.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MonitoringCenter ON SubQuery3.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY SubQuery3.deviceID DESC;


--Query the current and voltage of the device whose latest status is abnormal (-1) and whose device type is 'electricity meter':

SELECT SubQuery4.deviceID, ModelName, current, voltage
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.MonitoringCenter
            WHERE status = -1 AND ts IN (
                SELECT MAX(ts) 
                FROM tsdb.MonitoringCenter 
            )
        ) AS SubSubQuery4
    )
) AS SubQuery4
JOIN rdb.DeviceModel ON SubQuery4.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MeterSuper ON SubQuery4.deviceID = tsdb.MeterSuper.deviceID
WHERE TypeName = '电表'
ORDER BY current DESC, voltage DESC;


--Query the information and status of all devices whose frequency and temperature exceed the threshold:

SELECT SubQuery5.deviceID, ModelName, TypeName, status
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID 
        FROM (
            SELECT deviceID 
            FROM tsdb.TransformerSuper
            WHERE frequency > '20' AND temperature > '25'
        ) AS SubSubQuery5
    )
) AS SubQuery5
JOIN rdb.DeviceModel ON SubQuery5.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MonitoringCenter ON SubQuery5.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY SubQuery5.deviceID,status DESC;

--Query the number of devices in different regions:

SELECT location, COUNT(*) as device_count
FROM (
    SELECT location
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM rdb.Device
        ) AS SubSubQuery6
    )
) AS SubQuery6
GROUP BY location
ORDER BY device_count DESC;


--Query the information and status of all devices whose most recent recorded current and voltage are lower than the threshold:

SELECT SubQuery7.deviceID, ModelName, current, voltage, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.MeterSuper
            WHERE ts IN (
                SELECT MAX(ts)
                FROM tsdb.MeterSuper
            ) AND current < '100' AND voltage < '220'
        ) AS SubSubQuery7
    )
) AS SubQuery7
JOIN rdb.DeviceModel ON SubQuery7.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MeterSuper ON SubQuery7.deviceID = tsdb.MeterSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery7.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC;

--Query the device information and status of the device whose temperature exceeds the threshold recorded in the last hour:

SELECT SubQuery8.deviceID, ModelName, temperature, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.TransformerSuper
            WHERE ts > '2023-07-03'
        ) AS SubSubQuery8
    )
) AS SubQuery8
JOIN rdb.DeviceModel ON SubQuery8.modelID = rdb.DeviceModel.modelID
JOIN tsdb.TransformerSuper ON SubQuery8.deviceID = tsdb.TransformerSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery8.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY deviceid,temperature;

--Query the latest record of the device whose current and voltage are higher than the threshold and its latest status:

SELECT SubQuery9.deviceID, ModelName, current, voltage, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.MeterSuper
            WHERE ts IN (
                SELECT MAX(ts)
                FROM tsdb.MeterSuper
            ) AND current > '10' AND voltage > '210'
        ) AS SubSubQuery9
    )
) AS SubQuery9
JOIN rdb.DeviceModel ON SubQuery9.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MeterSuper ON SubQuery9.deviceID = tsdb.MeterSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery9.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC;


--Query the information and status of all devices whose latest recorded frequency and temperature exceed the threshold:

SELECT SubQuery10.deviceID, ModelName, frequency, temperature, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.TransformerSuper
            WHERE ts IN (
                SELECT MAX(ts)
                FROM tsdb.TransformerSuper
            ) AND frequency > '20' AND temperature > '30'
        ) AS SubSubQuery10
    )
) AS SubQuery10
JOIN rdb.DeviceModel ON SubQuery10.modelID = rdb.DeviceModel.modelID
JOIN tsdb.TransformerSuper ON SubQuery10.deviceID = tsdb.TransformerSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery10.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC;





--In the sub-table association query use case, the ordinary time series table MonitoringCenter is modified into a sub-table and the previous use case is used.


--inner JOIN

-- Get the device type and device model of all devices with status -1
SELECT d.deviceID,dm.TypeName, dm.ModelName
FROM rdb.DeviceModel AS dm
INNER JOIN rdb.Device AS d ON dm.modelID = d.modelID
INNER JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
WHERE mc.status = -1 ORDER by d.deviceID;


--left JOIN
-- Get the device ID, device model, and its last status (if any) for all devices
SELECT d.deviceID, dm.ModelName, mc.status
FROM rdb.Device AS d
LEFT JOIN rdb.DeviceModel AS dm ON d.modelID = dm.modelID
LEFT JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
WHERE mc.ts = (SELECT MAX(ts) FROM tsdb.MonitoringCenter WHERE deviceID = d.deviceID) ORDER by d.deviceID;


--right JOIN
-- Get the device status of all monitoring centers and their corresponding device models (if any)
SELECT d.deviceID,mc.status, dm.ModelName
FROM rdb.DeviceModel AS dm
RIGHT JOIN rdb.Device AS d ON dm.modelID = d.modelID
RIGHT JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID ORDER by d.deviceID,mc.status, dm.ModelName;


--full JOIN
-- Get the status of all devices and their corresponding device models (if any)
SELECT d.deviceID,mc.status, dm.ModelName
FROM rdb.DeviceModel AS dm
FULL JOIN rdb.Device AS d ON dm.modelID = d.modelID
FULL JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID ORDER by d.deviceID,mc.status, dm.ModelName;


--The subquery must appear in the select list


SELECT d.deviceID,
       (SELECT MAX(status) FROM tsdb.MonitoringCenter WHERE deviceID = d.deviceID) AS LatestStatus
FROM rdb.Device AS d ORDER by d.deviceID;


--The subquery must appear in the where clause

--Query detailed information about devices with monitoring data within a certain period of time:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MonitoringCenter 
    WHERE ts BETWEEN '2023-07-02' AND '2023-07-05'
) ORDER by deviceID,modelID,installDate,location;


--Query the monitoring status of all devices of a certain device model：

SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE modelID = 1
) ORDER by deviceID,status,ts;


--To query detailed information about a device in a fault state:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MonitoringCenter 
    WHERE status = -1
) ORDER by deviceID,modelID,installDate,location;



--Query the monitoring data of devices whose installation dates are within a certain range:

SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE installDate BETWEEN '2023-07-01' AND '2023-07-05'
) ORDER by deviceID,status,ts;


--Query the monitoring data of all devices in a certain area:

SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE location = 'area1'
)ORDER by deviceID,status,ts;


--To query detailed information about faulty devices in a certain area:

SELECT * FROM rdb.Device
WHERE deviceID IN (
    SELECT deviceID FROM tsdb.MonitoringCenter 
    WHERE status = -1
) AND location = 'area1' ORDER by deviceID,modelID,installDate,location;


--Query the monitoring data of all devices of a device model in a certain area:

SELECT * FROM tsdb.MonitoringCenter
WHERE deviceID IN (
    SELECT deviceID FROM rdb.Device 
    WHERE modelID IN (
        SELECT modelID FROM rdb.DeviceModel
        WHERE TypeName = '电表'
    ) AND location = 'area1'
)ORDER by deviceID,status,ts;



--Subquery in having SQL statement


--Subquery in exists


--Query the region where the device instances that exist in the Device table of the rdb library and have abnormal status in the MonitoringCenter table of the tsdb library are located:

SELECT d.location
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    AND mc.status = -1
) ORDER by d.location;


--The query exists in the Device table of the rdb library, and there are device models with the same device instance whose number of device instances is greater than 5 in the MonitoringCenter table of the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.ModelID = d.modelID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    GROUP BY mc.deviceID
    HAVING COUNT(*) > 5
);


--Get the model name and corresponding status of the device with status data, and sort it in descending order by status value:

SELECT dm.ModelName, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MonitoringCenter mc ON dm.modelID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc2
    WHERE mc2.deviceID = mc.deviceID
)
ORDER BY mc.status DESC;



--Get the device model name and the corresponding current and status data, and sort them in descending order of current and ascending order of status:

SELECT dm.ModelName, m.current, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper m2
    WHERE

 m2.deviceID = m.deviceID
)
ORDER BY m.current DESC, mc.status ASC;


--Get the model name and the corresponding current and status data, where the current is greater than 100 or the status is 1, and sort by current in descending order and status in ascending order:

SELECT dm.ModelName, m.current, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MeterSuper m2
    WHERE m2.deviceID = m.deviceID
)
    AND (m.current > 100 OR mc.status = 1)
ORDER BY m.current DESC, mc.status ASC;


--Get the installation date and corresponding voltage data of the electric meter devices with location 'area2' and existence status 0, and sort them in descending order by installation date:

SELECT d.installDate, m.voltage
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE d.location = 'area2'
    AND mc.status = 0
    AND EXISTS (
        SELECT 1
        FROM rdb.DeviceModel dm
        WHERE dm.modelID = d.modelID
            AND dm.TypeName = '电表'
    )
ORDER BY d.installDate DESC;


--Subquery in in

--Get the model name, type name, and corresponding current and status data of all devices, sorted in ascending order by device ID:

SELECT dm.ModelName, dm.TypeName, m.current, mc.status
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
JOIN tsdb.MonitoringCenter mc ON m.deviceID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM rdb.Device d
    WHERE d.modelID = dm.modelID
)
ORDER BY m.deviceID ASC,m.current;

--Get the model names and corresponding current data of devices with existence status 1, and sort them in ascending order by model name:

SELECT dm.ModelName, m.current
FROM rdb.DeviceModel dm
JOIN tsdb.MeterSuper m ON dm.modelID = m.modelID
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = m.deviceID
        AND mc.status = 1
)
ORDER BY dm.ModelName ASC;
--Get the model name and corresponding current data of the device with position 'area2' and existence status 0, and sort them in descending order by current:

SELECT dm.ModelName, m.current
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE d.location = 'area2'
    AND mc.status = 0
ORDER BY m.current DESC;


--Get the location and corresponding voltage data of devices with installation date after '2023-05-01' and existence status 1, sorted by voltage in ascending order:

SELECT d.location, m.voltage
FROM rdb.Device d
JOIN tsdb.MeterSuper m ON d.deviceID = m.deviceID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE d.installDate > '2023-05-01'
    AND mc.status = 1
ORDER BY m.voltage ASC;

--The scalar subquery must appear in the select list.

--Query the device ID of each device instance in the Device table of the rdb library and the number of states of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT COUNT(mc.status)
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID) AS StatusCount
FROM rdb.Device d order by d.deviceID;


--Query the device ID of each device instance in the Device table of the rdb library and the latest status of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT mc.status
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID
     ORDER BY mc.ts DESC
     LIMIT 1) AS LatestStatus
FROM rdb.Device d order by deviceid;


--Query the device ID of each device instance in the Device table of the rdb library and the earliest status time of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT MIN(mc.ts)
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID) AS EarliestStatusTime
FROM rdb.Device d order by d.deviceID;

--Query the device ID of each device instance in the Device table of the rdb library and the status average of the same device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID,
    (SELECT AVG(mc.status)
     FROM tsdb.MonitoringCenter mc
     WHERE mc.deviceID = d.deviceID) AS AverageStatus
FROM rdb.Device d order by d.deviceID;

--Get the location and corresponding status value of each device. The number of devices with status value 1 is greater than 2. Sort in descending order by location name:

SELECT d.location,
       (SELECT mc.status FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = d.deviceID) AS Status
FROM rdb.Device d
WHERE (SELECT COUNT(*) FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = d.deviceID AND mc.status = 1) > 2
ORDER BY d.location DESC;

--Scalar subquery in where

--Query the device ID in the Device table of the rdb library whose device installation date is earlier than the earliest status time of a device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE d.installDate < (
    SELECT MIN(mc.ts)
    FROM tsdb.MonitoringCenter mc
);

--Query the device ID in the Device table of the rdb library whose device installation date is equal to the earliest status time of a device instance in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE d.installDate = (
    SELECT MIN(mc.ts)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = (
        SELECT d.deviceID
        FROM rdb.Device d ORDER by d.deviceID limit 1
    )
);


--Query the device model whose device ID in the Device table of the rdb library is equal to the device ID of a device instance in the MonitoringCenter table of the tsdb library:

SELECT dm.TypeName
FROM rdb.DeviceModel dm
WHERE dm.ModelID = (
    SELECT d.modelID
    FROM rdb.Device d
    WHERE d.deviceID = (
        SELECT mc.deviceID
        FROM tsdb.MonitoringCenter mc ORDER by mc.deviceID limit 1
    )
);

--Get the number of devices of each device model and the number of devices with the corresponding status 1, and sort them in descending order by the number of devices:

SELECT dm.ModelName,
       (SELECT COUNT(*) FROM rdb.Device d WHERE d.modelID = dm.modelID) AS DeviceCount,
       (SELECT COUNT(*) FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = ANY(SELECT d.deviceID FROM rdb.Device d WHERE d.modelID = dm.modelID) AND mc.status = 1) AS StatusCount
FROM rdb.DeviceModel dm
ORDER BY DeviceCount DESC, dm.ModelName;


--Scalar subquery in having

--Query the device ID of each device instance in the Device table of the rdb library, where the device ID exists in the MonitoringCenter table of the tsdb library and the number of states is greater than 5:

SELECT d.deviceID
FROM rdb.Device d
GROUP BY d.deviceID
HAVING d.deviceID IN (
    SELECT mc.deviceID
    FROM tsdb.MonitoringCenter mc
    GROUP BY mc.deviceID
    HAVING COUNT(*) > 5
);

--Query the device ID and device model of each device instance in the Device table of the rdb library, where the status average value of the device instance with the same device ID in the MonitoringCenter table of the tsdb library is less than -1:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
GROUP BY d.deviceID, dm.TypeName
HAVING AVG(d.deviceID) < (
    SELECT AVG(mc.status)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
);

--Get the number of devices of each device model and the number of devices with the corresponding status of 1. If the number of devices is greater than 2, sort them in descending order by the number of devices:

SELECT dm.ModelName,
COUNT(1) AS DeviceCount,
(SELECT COUNT(1) FROM tsdb.MonitoringCenter mc WHERE mc.deviceID = d.deviceID AND mc.status = 1) AS StatusCount
FROM rdb.DeviceModel dm
JOIN rdb.Device d ON dm.modelID = d.modelID
GROUP BY dm.ModelName, d.deviceID
HAVING COUNT(*) > 2
ORDER BY DeviceCount DESC;

--Scalar subquery in having

--Get the model names and corresponding temperature values of devices whose status is 0 and current is less than 100, and sort them in descending order by temperature value:

SELECT dm.ModelName,
t.temperature AS Temperature
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.modelID
JOIN tsdb.TransformerSuper t ON d.deviceID = t.deviceID
WHERE d.deviceID IN (SELECT mc.deviceID FROM tsdb.MonitoringCenter mc WHERE mc.status = 0)
AND (SELECT MAX(m.current) FROM tsdb.MeterSuper m WHERE m.deviceID = d.deviceID) < 100
ORDER BY Temperature DESC;


--Get the location and corresponding status value of each device, where the number of devices is greater than 1, and sort in descending order by location name:

SELECT d.location,
       mc.status AS Status
FROM rdb.Device d
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE EXISTS (
    SELECT 1
    FROM rdb.Device d2
    WHERE d2.location = d.location
    HAVING COUNT(*) > 1
)
ORDER BY d.location DESC,status;


--Scalar subquery in order by


--Query the device ID and device model in the Device table of the rdb library, and sort in ascending order according to the number of states with the same device ID in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY (
    SELECT COUNT(mc.status)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
) ASC, d.deviceID;

--Query the device ID and device model in the Device table of the rdb library, and sort in ascending order according to the earliest status time of the same device ID in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY (
    SELECT MIN(mc.ts)
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
) ASC, d.deviceID;


--The projection subquery must be a single column

--Query the device ID in the Device table of the rdb library, where the number of records with states greater than 10 exists in the MonitoringCenter table of the tsdb library:

SELECT d.deviceID
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    HAVING COUNT(*) > 10
);


--Query the device ID in the Device table of the rdb library, and find the record with status 0 in the MonitoringCenter table of the tsdb library

SELECT d.deviceID
FROM rdb.Device d
WHERE EXISTS (
    SELECT 1
    FROM tsdb.MonitoringCenter mc
    WHERE mc.deviceID = d.deviceID
    AND mc.status = 0
) order by deviceID;


--The projection subquery must have multiple columns.


--Query the device ID and device model in the Device table of the rdb library, where the records with a status quantity greater than 10 exist in the MonitoringCenter table of the tsdb library, and return the status average and maximum values ​​of the corresponding device ID in the tsdb library:

SELECT d.deviceID, dm.TypeName, AVG(mc.status), MAX(mc.status)
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
GROUP BY d.deviceID, dm.TypeName
HAVING COUNT(mc.status) > 10;


--Query the device ID and device model in the Device table of the rdb library, where the records with a status of 0 exist in the MonitoringCenter table of the tsdb library, and return the average and minimum status values ​​of the corresponding device ID in the tsdb library:

SELECT d.deviceID, dm.TypeName, AVG(mc.status), MIN(mc.status)
FROM rdb.Device d
JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
JOIN tsdb.MonitoringCenter mc ON d.deviceID = mc.deviceID
WHERE mc.status = 0
GROUP BY d.deviceID, dm.TypeName order by d.deviceID;


--union query SQL statement

--Query the device ID and timestamp in the Device table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT deviceID, NULL AS ts
FROM rdb.Device
UNION
SELECT NULL AS deviceID, ts
FROM tsdb.MonitoringCenter order by deviceID, ts;


--Query the device ID and status in the Device table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT deviceID, NULL AS status
FROM rdb.Device
UNION
SELECT NULL AS deviceID, status
FROM tsdb.MonitoringCenter order by deviceID, status;


--Query the model type and status in the DeviceModel table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT TypeName, NULL AS status
FROM rdb.DeviceModel
UNION
SELECT NULL AS TypeName, status
FROM tsdb.MonitoringCenter order by TypeName, status;


--Query the device location and timestamp in the Device table of the rdb library and the MonitoringCenter table of the tsdb library:

SELECT location, NULL AS ts
FROM rdb.Device
UNION
SELECT NULL AS location, ts
FROM tsdb.MonitoringCenter order by location,ts;



--Cross-database union all query SQL statement


--Query the Device table in the rdb library and the MonitoringCenter table in the tsdb library, and sort them in ascending order by device ID:

SELECT deviceID, installDate, location, NULL AS ts, NULL AS status
FROM rdb.Device
UNION ALL
SELECT NULL AS deviceID, NULL AS installDate, NULL AS location, ts, status
FROM tsdb.MonitoringCenter
ORDER BY deviceID, installDate, location ASC,ts;


--Query the DeviceModel table in the rdb library and the MonitoringCenter table in the tsdb library, and sort them in ascending order by model ID:

SELECT ModelID, TypeID, ModelName, TypeName, NULL AS ts, NULL AS status
FROM rdb.DeviceModel
UNION ALL
SELECT NULL AS ModelID, NULL AS TypeID, NULL AS ModelName, NULL AS TypeName, ts, status
FROM tsdb.MonitoringCenter
ORDER BY ModelID ASC, ts;

--Query the Device table of the rdb library and the MonitoringCenter table of the tsdb library, and sort them in descending order by installation date:

SELECT deviceID, installDate, location, NULL AS ts, NULL AS status
FROM rdb.Device
UNION ALL
SELECT NULL AS deviceID, NULL AS installDate, NULL AS location, ts, status
FROM tsdb.MonitoringCenter
ORDER BY installDate DESC,deviceid,ts;


--Query the DeviceModel table of the rdb library and the MonitoringCenter table of the tsdb library, and sort them in descending order by type ID:

SELECT ModelID, TypeID, ModelName, TypeName, NULL AS ts, NULL AS status
FROM rdb.DeviceModel
UNION ALL
SELECT NULL AS ModelID, NULL AS TypeID, NULL AS ModelName, NULL AS TypeName, ts, status
FROM tsdb.MonitoringCenter
ORDER BY ModelID,  ts DESC;

--Cross-database intersect query SQL statement


-- 1. Query the deviceID that appears in both rdb.Device and tsdb.MonitoringCenter
SELECT deviceID FROM rdb.Device
INTERSECT
SELECT deviceID FROM tsdb.MonitoringCenter
ORDER BY deviceID;

-- 8. Query deviceID that appears in both tsdb.MeterSuper and tsdb.MonitoringCenter
SELECT deviceID FROM tsdb.MeterSuper
INTERSECT
SELECT deviceID FROM tsdb.MonitoringCenter
ORDER BY deviceID DESC;

-- 9. Query deviceID that appears in both tsdb.TransformerSuper and tsdb.MonitoringCenter
SELECT deviceID FROM tsdb.TransformerSuper
INTERSECT
SELECT deviceID FROM tsdb.MonitoringCenter
ORDER BY deviceID;


--Cross-database intersect all query SQL statement

--Cross-database except query SQL statement

--Cross-library except all


--Multi-level nested subqueries


--Query the type and location of devices whose latest status is normal (0):

SELECT DISTINCT TypeName, location
FROM (
    SELECT modelID, location 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID, status
            FROM tsdb.MonitoringCenter
            WHERE ts IN (
                SELECT MAX(ts) 
                FROM tsdb.MonitoringCenter 
                WHERE status = 0
            )
        ) AS SubSubQuery1
    )
) AS SubQuery1
JOIN rdb.DeviceModel ON SubQuery1.modelID = rdb.DeviceModel.modelID
ORDER BY TypeName, location;


--Query the information and status of all devices whose current and voltage exceed the threshold:

SELECT SubQuery2.deviceID, ModelName, TypeName, status
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID 
        FROM (
            SELECT deviceID 
            FROM tsdb.MeterSuper
            WHERE current > '10' AND voltage > '220'
        ) AS SubSubQuery2
    )
) AS SubQuery2
JOIN rdb.DeviceModel ON SubQuery2.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MonitoringCenter ON SubQuery2.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC, SubQuery2.deviceID;


--Query the information of the device with the earliest installation date and its latest status:

SELECT SubQuery3.deviceID, ModelName, TypeName, status
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE installDate IN (
        SELECT MIN(installDate) 
        FROM (
            SELECT installDate 
            FROM rdb.Device
        ) AS SubSubQuery3
    )
) AS SubQuery3
JOIN rdb.DeviceModel ON SubQuery3.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MonitoringCenter ON SubQuery3.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY SubQuery3.deviceID DESC;


--Query the current and voltage of the device whose latest status is abnormal (-1) and whose device type is 'electricity meter':

SELECT SubQuery4.deviceID, ModelName, current, voltage
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.MonitoringCenter
            WHERE status = -1 AND ts IN (
                SELECT MAX(ts) 
                FROM tsdb.MonitoringCenter 
            )
        ) AS SubSubQuery4
    )
) AS SubQuery4
JOIN rdb.DeviceModel ON SubQuery4.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MeterSuper ON SubQuery4.deviceID = tsdb.MeterSuper.deviceID
WHERE TypeName = '电表'
ORDER BY current DESC, voltage DESC;


--Query the information and status of all devices whose frequency and temperature exceed the threshold:

SELECT SubQuery5.deviceID, ModelName, TypeName, status
FROM (
    SELECT modelID, deviceID 
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID 
        FROM (
            SELECT deviceID 
            FROM tsdb.TransformerSuper
            WHERE frequency > '20' AND temperature > '25'
        ) AS SubSubQuery5
    )
) AS SubQuery5
JOIN rdb.DeviceModel ON SubQuery5.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MonitoringCenter ON SubQuery5.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC, SubQuery5.deviceID;

--Query the number of devices in different regions:

SELECT location, COUNT(*) as device_count
FROM (
    SELECT location
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM rdb.Device
        ) AS SubSubQuery6
    )
) AS SubQuery6
GROUP BY location
ORDER BY device_count DESC;


--Query the information and status of all devices whose most recent recorded current and voltage are lower than the threshold:

SELECT SubQuery7.deviceID, ModelName, current, voltage, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.MeterSuper
            WHERE ts IN (



                SELECT MAX(ts)
                FROM tsdb.MeterSuper
            ) AND current < '100' AND voltage < '220'
        ) AS SubSubQuery7
    )
) AS SubQuery7
JOIN rdb.DeviceModel ON SubQuery7.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MeterSuper ON SubQuery7.deviceID = tsdb.MeterSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery7.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC;

--Query the device information and status of the device whose temperature exceeds the threshold recorded in the last hour:

SELECT SubQuery8.deviceID, ModelName, temperature, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.TransformerSuper
            WHERE ts > '2023-07-03'
        ) AS SubSubQuery8
    )
) AS SubQuery8
JOIN rdb.DeviceModel ON SubQuery8.modelID = rdb.DeviceModel.modelID
JOIN tsdb.TransformerSuper ON SubQuery8.deviceID = tsdb.TransformerSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery8.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY deviceid,temperature;

--Query the latest record of the device whose current and voltage are higher than the threshold and its latest status:

SELECT SubQuery9.deviceID, ModelName, current, voltage, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.MeterSuper
            WHERE ts IN (
                SELECT MAX(ts)
                FROM tsdb.MeterSuper
            ) AND current > '10' AND voltage > '210'
        ) AS SubSubQuery9
    )
) AS SubQuery9
JOIN rdb.DeviceModel ON SubQuery9.modelID = rdb.DeviceModel.modelID
JOIN tsdb.MeterSuper ON SubQuery9.deviceID = tsdb.MeterSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery9.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC;


--Query the information and status of all devices whose latest recorded frequency and temperature exceed the threshold:

SELECT SubQuery10.deviceID, ModelName, frequency, temperature, status
FROM (
    SELECT modelID, deviceID
    FROM rdb.Device
    WHERE deviceID IN (
        SELECT deviceID
        FROM (
            SELECT deviceID
            FROM tsdb.TransformerSuper
            WHERE ts IN (
                SELECT MAX(ts)
                FROM tsdb.TransformerSuper
            ) AND frequency > '20' AND temperature > '30'
        ) AS SubSubQuery10
    )
) AS SubQuery10
JOIN rdb.DeviceModel ON SubQuery10.modelID = rdb.DeviceModel.modelID
JOIN tsdb.TransformerSuper ON SubQuery10.deviceID = tsdb.TransformerSuper.deviceID
JOIN tsdb.MonitoringCenter ON SubQuery10.deviceID = tsdb.MonitoringCenter.deviceID
ORDER BY status DESC;

--Reference the most complex SQL syntax on site
SELECT d.deviceID, dm.ModelName, mc.status
FROM rdb.Device AS d
JOIN rdb.DeviceModel AS dm ON d.modelID = dm.ModelID
JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
WHERE d.deviceID IN (
  SELECT deviceID
  FROM tsdb.MonitoringCenter
  WHERE status = 0
)
UNION 
SELECT ms.deviceID, dm.ModelName, NULL AS status
FROM tsdb.MeterSuper AS ms
JOIN rdb.DeviceModel AS dm ON ms.modelID = dm.ModelID
JOIN rdb.Device AS d ON ms.deviceID = d.deviceID
WHERE ms.voltage > (
  SELECT AVG(voltage)
  FROM tsdb.MeterSuper
)  
UNION
SELECT ts.deviceID, dm.ModelName, NULL AS status
FROM tsdb.TransformerSuper AS ts
JOIN rdb.DeviceModel AS dm ON ts.modelID = dm.ModelID
JOIN rdb.Device AS d ON ts.deviceID = d.deviceID
WHERE ts.temperature > (
  SELECT MAX(temperature)
  FROM tsdb.TransformerSuper
)
ORDER BY deviceID ASC, status;


--Perform union operation after JOIN

SELECT d.deviceID, dm.ModelName, mc.status
FROM rdb.Device AS d
JOIN rdb.DeviceModel AS dm ON d.modelID = dm.ModelID
JOIN tsdb.MonitoringCenter AS mc ON d.deviceID = mc.deviceID
UNION 
SELECT ms.deviceID, dm.ModelName, NULL AS status
FROM tsdb.MeterSuper AS ms
JOIN rdb.DeviceModel AS dm ON ms.modelID = dm.ModelID
JOIN rdb.Device AS d ON ms.deviceID = d.deviceID
UNION
SELECT ts.deviceID, dm.ModelName, NULL AS status  
FROM tsdb.TransformerSuper AS ts
JOIN rdb.DeviceModel AS dm ON ts.modelID = dm.ModelID
JOIN rdb.Device AS d ON ts.deviceID = d.deviceID
ORDER BY deviceID ASC,status;



--Perform JOIN operation after union



--Maximum number of association tables


--Live SQL
select STR_TO_DATE(hour,'%Y-%m-%d %H') as create_time,ifnull(vehicle.number,0) number from ( select * from (
select concat(a.thisweek,' ',b.hh) as hour from (
SELECT  DATE(subdate(curdate(),date_format(curdate(),'%w')-1)) as thisweek
union all
SELECT  DATE(DATE_ADD(subdate(curdate(),date_format(curdate(),'%w')-1), interval 1 day)) as thisweek
union all
SELECT  DATE(DATE_ADD(subdate(curdate(),date_format(curdate(),'%w')-1), interval 2 day)) as thisweek
union all
SELECT  DATE(DATE_ADD(subdate(curdate(),date_format(curdate(),'%w')-1), interval 3 day)) as thisweek
union all
SELECT  DATE(DATE_ADD(subdate(curdate(),date_format(curdate(),'%w')-1), interval 4 day)) as thisweek
union all
SELECT DATE(DATE_ADD(subdate(curdate(),date_format(curdate(),'%w')-1), interval 5 day)) as thisweek
union all
SELECT DATE(DATE_ADD(subdate(curdate(),date_format(curdate(),'%w')-1), interval 6 day)) as thisweek
) a left join (
SELECT *
FROM
(
SELECT hh
FROM
(
SELECT DATE_FORMAT(CURDATE(),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 1 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 2 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 3 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 4 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 5 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 6 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 7 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 8 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 9 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 10 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 11 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 12 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 13 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 14 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 15 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 16 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 17 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 18 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 19 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 20 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 21 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 22 HOUR),'%H') AS hh UNION
SELECT DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 23 HOUR),'%H') AS hh
) hourtable
) hourtable
) b on 1=1 order by a.thisweek,b.hh) c where hour <= DATE_FORMAT(now(),'%Y-%m-%d %H')) days left join (
select hh,h,count(*) number from (
select hh, min(create_time) h from (
select distinct date_format(create_time,'%Y-%m-%d %H') hh, create_time,vid from dcjg_vehicle_device_online_status where online = 1
) a
group by hh order by hh,h) tem1 left join dcjg_vehicle_device_online_status tem2 on tem1.h=tem2.create_time
where tem2.online = 1
group by hh,h) vehicle on days.hour= vehicle.hh
order by hour;

SELECT d.deviceID, dm.TypeName
FROM rdb.Device d
         JOIN rdb.DeviceModel dm ON d.modelID = dm.ModelID
ORDER BY 1,2,(
    SELECT MAX(ms.current) - MIN(ms.current)
    FROM tsdb.MeterSuper ms
    WHERE ms.deviceID = d.deviceID
) DESC;

SELECT ms.deviceID, ms.current,
       (SELECT AVG(ms2.current)
        FROM tsdb.MeterSuper ms2
        WHERE ms2.deviceID = ms.deviceID) AS AverageCurrent
FROM tsdb.MeterSuper ms ORDER BY ms.deviceID, ms.current;

-- ZDP-33574
INSERT INTO tsdb.MeterSuper VALUES ('2023-07-04 03:01:52.471061', 47.208762254501835, 210.62415836770043, 0.1394836664495358,005,5);
SELECT ms.deviceID, ms.ts
FROM tsdb.MeterSuper ms
WHERE EXISTS (
SELECT 1
FROM rdb.Device d
WHERE d.deviceID = ms.deviceID
AND d.installDate = ms.ts::date
) ORDER by ts;

-- clean data
reset extra_float_digits;
drop DATABASE rdb;
drop DATABASE tsdb cascade;

create ts database iot_gas_meter;
USE iot_gas_meter;
CREATE TABLE iot_gas_meter.meter_detail_info (
create_time                     TIMESTAMPTZ     NOT NULL,
read_time                   TIMESTAMPTZ     NOT NULL,
std_sum_vol             FLOAT4                  NOT NULL,
meter_status                INT8                    NULL,
event_code                  INT8                    NULL,
meter_time                  TIMESTAMPTZ             NOT NULL,
main_voltage                FLOAT4                  NULL,
eng_sum_vol                 FLOAT4                  NULL,
com_sum_vol                 FLOAT4                  NULL,
temperature                 FLOAT4                  NULL,
pressure                    INT8                    NULL,
acoustic_velocity   INT4                    NULL,
measure_voltage         FLOAT4                  NULL,
update_time                 TIMESTAMPTZ     NOT NULL
) TAGS (
meter_id                    INT8                    NOT NULL,
ou_id                           INT4                    NOT NULL,
mnft_code                       INT2                    NOT NULL
) PRIMARY TAGS(mnft_code, meter_id)
retentions 30d
ACTIVETIME 30d
partition interval 10d ;


CREATE TABLE iot_gas_meter.bill_abs_info (
 create_time                             TIMESTAMPTZ     NOT NULL,
 charge_time                                 TIMESTAMPTZ     NOT NULL,
 calc_amount                                     FLOAT8                  NULL,
 calc_volume                                 FLOAT8                  NULL,
 accumulate_volume                   FLOAT8                  NULL,
 meter_balance                               FLOAT8                  NULL,
 update_time                                 TIMESTAMPTZ     NOT NULL
) TAGS (
meter_id                                        INT8                    NOT NULL,
ou_id                                           INT4                    NOT NULL,
mnft_code                                       INT2                    NOT NULL
) PRIMARY TAGS(mnft_code, meter_id) retentions 30d partition interval 10d;


create database statistics;
create table statistics.tmp_uniq_id_lost (last_time TIMESTAMPTZ not null, meter_id int8 not null unique, ou_id int4, mnft_code int2);

explain select *
from meter_detail_info
where meter_id = '1252011120050116'
  and mnft_code = '125'
    limit 10;

explain select *
from meter_detail_info
where create_time >= TIMESTAMPTZ '2024-12-13 00:00:00.000+00:00'
  and meter_id = '1252011120050116'
  and mnft_code = '125'
limit 10;

explain select count(1)
from meter_detail_info
where create_time >=date_trunc('day', '2024-01-01 10:00:00'::timestamptz)-2h
  and create_time <date_trunc('day', '2024-01-01 10:00:00'::timestamptz)-1h;

set cluster setting sql.stats.ts_automatic_collection.enabled=true;

explain select meter_id, mnft_code, ou_id, count(1) as num
from meter_detail_info
where create_time >=date_trunc('day', '2024-01-01 10:00:00'::timestamptz)-2h
  and create_time <date_trunc('day', '2024-01-01 10:00:00'::timestamptz)-1h
  and ou_id = '138800'
group by meter_id, mnft_code , ou_id having count(1)>1;

reset cluster setting sql.stats.ts_automatic_collection.enabled;

drop database iot_gas_meter cascade;
drop database statistics cascade;
