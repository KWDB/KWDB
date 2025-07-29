-- create
SET CLUSTER SETTING server.advanced_distributed_operations.enabled = true;
SET cluster setting ts.rows_per_block.max_limit=10;
SET cluster setting ts.blocks_per_segment.max_limit=50;
SET CLUSTER SETTING kv.allocator.ts_consider_rebalance.enabled = true;

-- init
CREATE TS DATABASE tsdb;
ALTER DATABASE tsdb CONFIGURE ZONE USING gc.ttlseconds=10;
CREATE TABLE tsdb.t1(
ts TIMESTAMPTZ NOT NULL,e1 TIMESTAMP,e2 INT2,e3 INT4,e4 INT8,e5 FLOAT4,e6 FLOAT8,e7 BOOL,e8 CHAR,e10 NCHAR,e16 VARBYTES
) TAGS (
tag1 BOOL,tag2 SMALLINT,tag3 INT,tag4 BIGINT,tag5 FLOAT4,tag6 DOUBLE,tag7 VARBYTES,tag11 CHAR,tag13 NCHAR NOT NULL
)PRIMARY TAGS(tag13);
-- insert
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:10', '2024-02-06 07:53:45', 909, 872, 786, -7405.201085703498, 5680.235488974005, True, 'S', 'X', '1', True, 244, 821, 564, 7143.936442921109, 2561.374095811332, 'A', 't', 'r');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:11', '2024-02-06 07:53:45', 974, 94, 105, -5640.587402071648, -3281.6318342911745, True, 'Z', 'o', '8', False, 550, 44, 713, -6106.154077932764, -8543.06093658501, '1', 'F', 'p');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:12', '2024-02-06 07:53:45', 393, 16, 27, -123.54596030862012, 3175.129856074598, False, 'e', 'h', 'E', False, 481, 603, 642, 5232.021845605617, 7935.424987645434, 'E', 'S', 'j');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:13', '2024-02-06 07:53:45', 882, 363, 224, 1146.1157428047609, -6765.174812998099, True, 'I', 'c', '6', False, 902, 59, 215, 1950.083282081976, 6837.231892709842, '8', 'e', 'm');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:14', '2024-02-06 07:53:45', 775, 193, 641, -4361.142331906914, -3431.12583430383, True, 'J', 'e', 'A', False, 428, 61, 968, 4483.426111844052, -2524.2690513513226, '6', 's', 'b');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:15', '2024-02-06 07:53:45', 691, 853, 926, 5681.854192094452, 8139.085233329381, True, 'q', 'V', 'E', False, 725, 182, 484, -8127.888289931338, -3533.7807494110284, '8', 'H', 'b');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:16', '2024-02-06 07:53:45', 878, 837, 190, 2849.172415192703, -8786.966586983684, False, 'k', 'n', '6', False, 67, 72, 806, -182.95468164019803, 4618.2405974059275, 'B', 'e', 'v');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:17', '2024-02-06 07:53:45', 478, 716, 828, -4146.305443149258, -9290.013587091918, False, 'j', 'e', '9', False, 161, 725, 788, 2321.9028225105612, 3485.8811933866345, 'F', 'k', 'S');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:18', '2024-02-06 07:53:45', 443, 604, 890, 4934.296805170918, 2864.916526320325, False, 'X', 'F', '8', True, 262, 56, 893, -2642.9535835852657, 3461.0766841132754, '4', 'P', 'U');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:19', '2024-02-06 07:53:45', 101, 332, 338, 2422.3863331880493, 1971.7861089112066, True, 'G', 'i', 'F', False, 550, 641, 801, -1867.7105221420807, 2801.475585151429, '1', 'a', 'u');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:20', '2024-02-06 07:53:45', 949, 553, 684, 9713.860835053387, -2015.7845193886478, False, 'I', 'G', 'C', True, 17, 375, 406, 4058.5613145935604, 2239.9343196704376, 'E', 'p', 'O');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:21', '2024-02-06 07:53:45', 995, 821, 165, 4178.234401770713, 3089.7833111510827, True, 'x', 'q', '6', True, 805, 869, 910, 3631.830121636427, 4191.583481170079, '5', 'p', 'W');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:22', '2024-02-06 07:53:45', 713, 67, 557, 7453.404368683237, -9959.05851150546, True, 's', 'N', 'F', True, 158, 110, 876, 701.3537219282462, 5953.530872082236, '8', 'Y', 'w');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:23', '2024-02-06 07:53:45', 775, 171, 438, 1077.6945794974254, -7239.111724546714, True, 's', 'Q', '8', False, 84, 303, 454, -6296.565435102099, 3027.8338882661465, '1', 'g', 'L');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:24', '2024-02-06 07:53:45', 306, 405, 396, -8705.145530885276, 2475.0312082641976, True, 'w', 'u', 'E', True, 587, 43, 189, 3190.563773367807, -722.6277326503678, '0', 'l', 'K');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:25', '2024-02-06 07:53:45', 322, 410, 975, 8444.016215576048, -3218.499482593811, False, 'J', 'Z', '4', True, 10, 769, 746, -9098.734300656783, -3502.059937795958, 'D', 's', 'A');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:26', '2024-02-06 07:53:45', 64, 233, 963, -344.99167986596694, -1830.694518936475, True, 'r', 'J', 'E', False, 131, 613, 558, -8719.534734047309, -488.02747234845083, 'A', 's', 'r');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:27', '2024-02-06 07:53:45', 83, 876, 373, 8362.242654188562, -7691.419037309981, False, 'K', 'g', '7', True, 855, 569, 196, -6829.15548709981, 2644.417954202998, 'E', 'X', 'g');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:28', '2024-02-06 07:53:45', 597, 805, 114, 8974.996068433953, -2592.387373291481, True, 'D', 'k', '1', True, 195, 368, 227, -6669.431332531021, -6032.277105107358, 'E', 'h', 'x');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:29', '2024-02-06 07:53:45', 497, 237, 47, 5783.168613604959, -1263.9902975263158, True, 'z', 'x', 'F', False, 139, 160, 888, -9562.890474902886, 5967.017837897773, '8', 'L', 'j');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:30', '2024-02-06 07:53:45', 882, 79, 707, 509.92514787240725, -487.41254377647783, False, 'd', 'W', '9', True, 168, 843, 874, -1587.7615875550273, -2734.9126322350694, 'C', 'X', 'N');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:31', '2024-02-06 07:53:45', 895, 811, 512, -1567.0075952768875, -8188.658406579732, True, 'G', 't', '8', True, 338, 443, 653, 1641.6584866602807, 9437.751995439721, '4', 'm', 'S');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:32', '2024-02-06 07:53:45', 988, 745, 790, -6027.528284118156, -9257.464491391958, False, 'o', 'o', '7', True, 387, 124, 764, 2134.600898396655, 2388.4678971335834, '9', 'e', 'G');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:33', '2024-02-06 07:53:45', 945, 531, 915, -4532.964634891839, -937.1555858704232, False, 'O', 'D', '9', True, 517, 942, 843, -7136.763098800982, 1749.877724026419, '7', 'u', 'n');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:34', '2024-02-06 07:53:45', 335, 241, 184, 9776.57373836348, 268.469732343603, False, 'g', 'B', 'E', False, 642, 228, 605, -7806.872733721861, 6797.039993700211, 'F', 'K', 'v');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:35', '2024-02-06 07:53:45', 89, 977, 674, -3965.2277303971205, 881.7585618745888, True, 'P', 'G', '3', False, 73, 335, 975, -1502.1053929431455, -6778.934465021101, 'E', 'H', 'f');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:36', '2024-02-06 07:53:45', 601, 62, 701, -4027.1482931354076, -94.02970368482784, True, 'V', 'o', '3', True, 656, 414, 61, 7818.042321360281, 2076.1753232237897, '7', 'X', 'n');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:37', '2024-02-06 07:53:45', 52, 773, 868, 8.200978409451636, -5799.531408197383, False, 'R', 'v', 'A', True, 741, 239, 371, -7450.156493631302, 145.77589424288635, '4', 's', 'G');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:38', '2024-02-06 07:53:45', 492, 794, 461, -210.59619706347803, 5955.652257664717, True, 'l', 'v', '1', False, 653, 988, 803, -3003.530485811938, -5424.265229957128, 'A', 'F', 'v');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:39', '2024-02-06 07:53:45', 182, 855, 215, -8776.27407718639, 6880.439953866171, True, 'l', 'x', '8', False, 705, 595, 499, 7792.949744466168, 592.1944011482392, 'B', 'K', 'g');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:40', '2024-02-06 07:53:45', 207, 854, 383, 1913.3479655600477, 1110.2039857558266, False, 'P', 'M', '9', True, 982, 31, 788, 8134.777688934606, 9407.82546276963, 'D', 'i', 'c');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:41', '2024-02-06 07:53:45', 511, 121, 662, -9179.655703844952, -6848.491154796586, False, 'z', 'W', 'C', True, 589, 797, 487, -8409.125124030303, 2117.0776048644457, '0', 'Z', 'J');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:42', '2024-02-06 07:53:45', 151, 679, 944, 1367.252820576734, 1552.7459463652394, True, 'i', 'a', '1', True, 920, 303, 776, 4158.833389253781, -2342.406006929711, '9', 'Q', 'a');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:43', '2024-02-06 07:53:45', 275, 974, 351, -1297.3444423232231, -8175.453840196909, False, 'y', 'h', '6', False, 417, 971, 430, -1068.3018509808971, -8612.513075894189, 'C', 'R', 'G');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:44', '2024-02-06 07:53:45', 973, 862, 790, -5024.634215967921, 6891.075110550304, False, 'x', 'Z', 'B', False, 875, 18, 416, 3402.5489781336837, 7594.036259382108, 'A', 'i', 'H');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:45', '2024-02-06 07:53:45', 465, 349, 500, 3356.383464014132, -8529.173615047801, True, 'P', 'F', '0', True, 603, 613, 260, 2341.3532906528126, -6431.509253730878, '8', 'l', 'F');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:46', '2024-02-06 07:53:45', 192, 479, 739, -8118.423536655417, 9301.236892546061, False, 'I', 'k', '7', False, 692, 799, 48, 3317.4088841351204, -3684.9412481508507, 'F', 'o', 'E');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:47', '2024-02-06 07:53:45', 674, 836, 729, -9658.6179675579, -8802.521026900316, True, 'R', 'P', '9', True, 344, 602, 327, -2002.3207217507834, -6521.078285876738, 'F', 'M', 'C');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:48', '2024-02-06 07:53:45', 635, 560, 488, -299.9484669779831, -1623.338260008406, False, 'c', 'e', '6', False, 12, 580, 81, -7932.812539498595, 4924.941070583653, '3', 'M', 'R');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:49', '2024-02-06 07:53:45', 710, 231, 363, 3802.300619935335, 8385.52683779531, True, 'c', 'x', '6', False, 604, 874, 631, -7309.183422967682, 2509.7441330552138, '4', 'L', 'q');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:50', '2024-02-06 07:53:45', 437, 316, 875, -3413.3517640405216, 3089.9228878044632, True, 'O', 'y', 'C', True, 789, 254, 69, 3187.509000161548, -5199.879427100229, 'C', 'b', 'j');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:51', '2024-02-06 07:53:45', 256, 279, 528, 8969.414377499364, -749.26843944348, False, 'e', 'V', '6', False, 501, 761, 923, -1966.6900613130792, 2727.4937976183937, '3', 'i', 'D');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:52', '2024-02-06 07:53:45', 55, 87, 131, 4216.486809223377, 7653.455253519933, True, 'Y', 'P', 'F', False, 973, 97, 635, -8025.486191901554, -1899.401921120887, '8', 'Y', 'F');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:53', '2024-02-06 07:53:45', 958, 957, 757, 1307.8602796242358, -9147.225053843982, False, 'W', 'R', 'D', False, 172, 427, 354, 1275.639554810612, -5825.756957073305, 'D', 'E', 'R');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:54', '2024-02-06 07:53:45', 197, 340, 1000, 7865.173001407988, -1560.0735595984843, False, 'x', 'g', 'E', False, 576, 772, 148, -9429.595917615017, 2296.5384236604805, 'D', 'C', 'U');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:55', '2024-02-06 07:53:45', 943, 298, 481, -9295.931462827662, 983.6430589044376, True, 'F', 'C', '7', False, 143, 287, 701, 211.13774737319181, 282.1488237840895, 'A', 'W', 'W');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:56', '2024-02-06 07:53:45', 251, 450, 772, -5434.429844735949, 5528.974663582505, False, 'a', 'U', '1', False, 698, 398, 202, -2658.6441029827192, 949.6549312231509, 'B', 'q', 'j');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:57', '2024-02-06 07:53:45', 978, 659, 452, 6619.516949765453, -2019.995194062296, False, 'H', 'T', '2', False, 458, 69, 20, -3505.736904248737, -8255.125032564061, 'C', 'Q', 's');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:58', '2024-02-06 07:53:45', 488, 899, 700, 5306.011744760863, 5180.515783480678, True, 'c', 'Y', '9', False, 721, 727, 361, -3331.631097276435, -6863.908390989263, 'E', 'Q', 'w');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:10:59', '2024-02-06 07:53:45', 753, 595, 389, -8498.317489711293, -202.44340623434073, True, 'r', 'W', 'F', True, 336, 783, 896, 763.4394013013116, -8065.546785779027, 'A', 'y', 'U');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:00', '2024-02-06 07:53:45', 492, 374, 345, -9035.905000233422, 269.6662875152597, True, 'f', 'j', 'B', False, 104, 503, 60, 9570.964088201737, 8103.383667266509, '1', 'g', 'N');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:01', '2024-02-06 07:53:45', 267, 66, 668, 5090.068324398922, 8552.450654374585, True, 'W', 'Q', '5', True, 223, 963, 597, 5101.374706852726, -3447.8357093998384, 'E', 'p', 'G');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:02', '2024-02-06 07:53:45', 289, 305, 529, 5931.962964121287, -1348.010040677771, False, 'D', 'e', 'D', True, 642, 557, 114, 5392.437374649655, -2269.9248980547245, '3', 'q', 'n');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:03', '2024-02-06 07:53:45', 911, 637, 279, -5069.245470214827, -9459.725688915292, True, 'g', 'V', 'E', False, 873, 623, 289, 3014.3619488145614, 1240.7480603019376, 'D', 'l', 'K');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:04', '2024-02-06 07:53:45', 183, 691, 413, 4708.85443572125, 6928.616470953555, False, 'D', 'a', '7', False, 173, 158, 879, -1639.5437140147842, 3812.880863578199, 'A', 'G', 'q');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:05', '2024-02-06 07:53:45', 408, 70, 247, 3257.86384050945, 8166.251378611334, False, 'C', 'f', 'F', True, 28, 696, 790, 5443.4042934796325, 5383.928864727648, '6', 'z', 'I');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:06', '2024-02-06 07:53:45', 978, 190, 990, 2118.7106854885315, -5122.805046513794, False, 't', 'h', '5', True, 890, 472, 160, 4454.214367786713, 1481.0065132267919, '5', 'X', 'T');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:07', '2024-02-06 07:53:45', 697, 841, 411, 4246.112000949155, 4108.706001225826, True, 'u', 'm', '3', True, 177, 467, 964, -4307.584276273817, -4533.77730866132, 'C', 'W', 'd');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:08', '2024-02-06 07:53:45', 330, 169, 861, 8601.891708289284, -758.8986256181452, False, 'z', 'C', '6', False, 704, 265, 859, -5821.592974147753, -377.18364086608926, '5', 'B', 'c');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:09', '2024-02-06 07:53:45', 855, 217, 9, -4190.737293547593, 249.22107875586516, True, 'Z', 'm', '4', True, 339, 784, 573, -9351.055526133716, -4843.15909036475, 'F', 'k', 'G');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:10', '2024-02-06 07:53:45', 558, 824, 147, -4360.581518656785, 674.8752490834959, True, 'y', 'F', 'C', False, 195, 945, 152, -7509.367231938615, -3.549630999106739, '4', 'p', 'T');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:11', '2024-02-06 07:53:45', 258, 712, 536, 5989.614891415993, -2522.6955662774535, True, 'R', 'e', 'C', False, 869, 474, 716, 8765.945544846247, 9282.210008164016, '7', 'o', 'L');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:12', '2024-02-06 07:53:45', 468, 794, 89, 1255.70609851289, 8699.998772396702, False, 'M', 'z', '3', True, 239, 698, 944, -3415.3347610130004, 2234.47837294945, 'F', 'B', 'a');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:13', '2024-02-06 07:53:45', 601, 744, 550, -5445.516501261997, 6397.658997246308, False, 'z', 'F', 'D', False, 674, 34, 404, -3585.8958878233225, 6662.151407689893, '5', 'V', 'n');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:14', '2024-02-06 07:53:45', 874, 711, 951, -7091.236302766774, 9736.57963973616, False, 'C', 'e', '7', True, 357, 611, 878, -3631.1892122777253, 8677.82790654826, 'E', 'P', 'D');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:15', '2024-02-06 07:53:45', 441, 106, 864, 672.4221224812245, 2758.4530519047294, True, 'q', 's', '3', True, 193, 793, 714, 7901.6457292034465, -5467.197356379321, '7', 'n', 'P');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:16', '2024-02-06 07:53:45', 613, 272, 858, -8211.334695085008, -4447.938976246118, True, 'Z', 'D', 'B', False, 91, 263, 363, -5306.424738424483, 2778.9223503095436, 'F', 'Z', 'f');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:17', '2024-02-06 07:53:45', 384, 998, 289, -6078.941801819544, -6379.266782822208, False, 'v', 't', 'A', True, 433, 566, 771, 5896.257601352474, 1041.8151332471734, '0', 'p', 'l');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:18', '2024-02-06 07:53:45', 312, 421, 626, 3601.7618553168577, -4810.469160528599, True, 'T', 'J', '0', False, 728, 659, 835, 7092.2533197177545, 26.495865439470435, 'A', 'q', 'y');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:19', '2024-02-06 07:53:45', 867, 582, 218, 7757.257340318596, 4419.656807454941, True, 'V', 'e', '5', False, 227, 329, 745, -5946.8768532374215, 9337.403474861538, '6', 'F', 'Q');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:20', '2024-02-06 07:53:45', 664, 159, 245, 9614.120605460423, -7038.830263304229, True, 'L', 'y', 'A', False, 56, 877, 774, 7362.917041026933, 9695.994389618154, 'F', 'E', 'Z');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:21', '2024-02-06 07:53:45', 102, 238, 741, -1905.0334325381682, -2789.9817007857, False, 'I', 'u', 'E', False, 755, 796, 252, 2390.9031900654027, -2784.7359184912275, '0', 'h', 'q');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:22', '2024-02-06 07:53:45', 241, 50, 118, 2792.3678628224297, 8061.1193627726425, True, 'l', 'c', 'F', True, 991, 663, 62, -5545.866910745587, -7763.056852156163, '2', 'q', 'A');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:23', '2024-02-06 07:53:45', 239, 809, 229, 9068.672270871331, 4840.211878271073, False, 'x', 'W', 'E', False, 582, 491, 570, 4202.0539035745805, -7947.261500659586, '0', 'f', 'F');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:24', '2024-02-06 07:53:45', 267, 70, 339, -9419.470576221458, 7753.830148600897, True, 'p', 'k', 'F', True, 146, 641, 60, -1132.4537159217634, 6496.409593943747, '0', 'c', 'L');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:25', '2024-02-06 07:53:45', 274, 285, 6, 3190.8549204136107, 8986.899140432815, True, 'a', 'q', '0', False, 401, 724, 29, -7679.588577213241, 5964.770431221936, 'D', 'g', 'W');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:26', '2024-02-06 07:53:45', 96, 202, 324, 8307.584500559875, 5716.653117548092, False, 'O', 'G', 'C', False, 623, 42, 844, -9642.861533845622, 2298.7479202912928, '9', 'K', 'J');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:27', '2024-02-06 07:53:45', 160, 86, 355, 5156.482878347992, 8472.541435098668, True, 'R', 'g', 'F', False, 904, 939, 734, 620.7917680433111, 3029.970031263727, '7', 'H', 'g');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:28', '2024-02-06 07:53:45', 933, 196, 425, -9871.83682136752, 9517.061098977374, False, 'w', 'Q', 'E', True, 154, 668, 150, -9776.540880008926, 5175.013149800454, 'A', 'Y', 'g');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:29', '2024-02-06 07:53:45', 876, 7, 667, -8404.820059520653, -4292.744671977644, False, 'D', 'f', '9', False, 464, 177, 367, -699.912111190908, 8577.981510198279, '7', 'N', 'b');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:30', '2024-02-06 07:53:45', 657, 43, 669, 4166.960552734981, 81.76232661469658, True, 'N', 'O', '5', True, 892, 159, 369, 6248.61042348933, 5997.775564531254, '5', 'c', 'f');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:31', '2024-02-06 07:53:45', 904, 860, 49, -5196.629218171512, -703.223183673339, False, 'N', 'c', '2', True, 115, 906, 443, -1641.1513448305886, -2671.6411758341583, 'E', 'x', 'U');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:32', '2024-02-06 07:53:45', 462, 204, 278, -1980.760999578506, -844.4941235726455, True, 'z', 'k', '7', False, 82, 907, 386, 4080.154551211208, 3662.7440587624624, 'C', 'X', 'z');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:33', '2024-02-06 07:53:45', 933, 521, 821, 3257.190408913424, 441.5878872432695, True, 'B', 'd', '2', True, 793, 353, 818, -7236.40374736571, 516.6248495790442, '0', 'W', 'l');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:34', '2024-02-06 07:53:45', 197, 472, 485, 873.461669283668, 4826.326450644543, False, 'x', 'T', '1', False, 309, 712, 982, -9287.595697137198, -2717.562136688225, '9', 'h', 'I');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:35', '2024-02-06 07:53:45', 807, 809, 689, -9725.514125214366, 792.5396886367162, False, 'w', 'h', '7', True, 183, 267, 181, -371.88923773778515, -6359.159264364911, '2', 'f', 'A');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:36', '2024-02-06 07:53:45', 616, 359, 779, -5449.561648870706, 1739.636103458697, False, 'e', 's', '9', False, 536, 591, 749, 9326.229403152916, -3901.4303975893054, 'B', 'F', 'f');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:37', '2024-02-06 07:53:45', 471, 1000, 908, 2709.002583228381, -365.3302897189351, False, 'a', 'n', '9', False, 466, 799, 918, 5309.641711899834, -9858.452567822735, 'B', 'b', 'e');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:38', '2024-02-06 07:53:45', 6, 502, 968, 7469.206334041588, 289.2122262819594, False, 'X', 'v', 'F', False, 878, 39, 97, -1601.8153833301076, -867.7588131323992, 'B', 'o', 'i');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:39', '2024-02-06 07:53:45', 192, 30, 459, 1253.7104626909131, -1787.6174774207739, True, 'X', 'A', '3', True, 540, 4, 457, -4503.866612958711, 8430.998084574065, 'A', 'J', 'm');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:40', '2024-02-06 07:53:45', 338, 556, 423, 9578.697562056757, -7948.157685240998, True, 'g', 'e', '1', True, 876, 213, 93, -6828.181285751975, -6761.860517173975, '3', 'B', 'M');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:41', '2024-02-06 07:53:45', 358, 22, 66, 9280.385666649177, -7461.391210461741, True, 's', 'C', '9', False, 566, 564, 939, -2817.6339365181448, 5902.445009472865, 'C', 'U', 'o');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:42', '2024-02-06 07:53:45', 525, 971, 640, 2524.534083642131, 6526.327597428124, False, 'x', 'n', '5', True, 734, 607, 271, -4854.01132795148, -7530.10477671791, '3', 'X', 'N');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:43', '2024-02-06 07:53:45', 15, 207, 339, 9827.29698647606, -2116.215255383795, False, 'd', 'C', '2', True, 778, 987, 441, -9533.318807991922, -560.2703687483881, '4', 'q', 'R');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:44', '2024-02-06 07:53:45', 869, 706, 186, 4112.128998756922, 7450.3734387301265, True, 'm', 'Y', '0', True, 927, 462, 507, -8230.102177296345, -1785.1714345840446, '6', 'r', 'f');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:45', '2024-02-06 07:53:45', 350, 204, 478, 7088.756643818913, 897.4126161663662, True, 'n', 'V', '7', True, 297, 165, 406, 5724.929926544155, 9047.026806111626, 'C', 'g', 'v');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:46', '2024-02-06 07:53:45', 338, 486, 453, -2003.093109218621, 806.5455882476399, True, 'b', 'A', 'D', False, 130, 907, 311, -7055.8759590601385, 2117.3746049132933, 'B', 'Q', 'Y');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:47', '2024-02-06 07:53:45', 800, 621, 468, 8757.687332614038, 5405.484411985166, False, 'h', 'V', '2', False, 278, 231, 686, 9348.570982891488, -4284.130455467294, '3', 'b', 'G');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:48', '2024-02-06 07:53:45', 35, 164, 966, 8873.539765774418, -6295.051624778738, False, 'b', 'J', '6', True, 967, 656, 150, 7536.393986761272, 4845.217063334514, '6', 'j', 'u');
INSERT INTO tsdb.t1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e10, e16, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag11, tag13) VALUES ('2000-10-10T10:11:49', '2024-02-06 07:53:45', 611, 965, 5, 7567.450800110706, -2746.5781895453874, True, 's', 'q', '6', True, 231, 311, 211, 8294.536259146378, 1809.1062291211329, 'A', 'u', 'q');
SELECT * FROM tsdb.t1 ORDER BY ts;
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;

CREATE TABLE tsdb.t2(
                        ts TIMESTAMPTZ NOT NULL,e1 TIMESTAMP,e2 INT2,e3 INT4,e4 INT8,e5 FLOAT4,e6 FLOAT8,e7 BOOL,e8 CHAR,e10 NCHAR,e16 VARBYTES
) TAGS (
tag1 BOOL,tag2 SMALLINT,tag3 INT,tag4 BIGINT,tag5 FLOAT4,tag6 DOUBLE,tag7 VARBYTES,tag11 CHAR,tag13 NCHAR NOT NULL
)PRIMARY TAGS(tag13);
DROP TABLE tsdb.t2;

SELECT * FROM tsdb.t1 ORDER BY ts;
-- join: c6
-- sleep: 10s
-- wait-running: c1
SELECT node_id,decommissioning FROM kwdb_internal.gossip_liveness ORDER BY node_id;
SELECT * FROM tsdb.t1 ORDER BY ts;
SELECT count(*) FROM tsdb.t1;
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;

CREATE TABLE tsdb.t2(
                        ts TIMESTAMPTZ NOT NULL,e1 TIMESTAMP,e2 INT2,e3 INT4,e4 INT8,e5 FLOAT4,e6 FLOAT8,e7 BOOL,e8 CHAR,e10 NCHAR,e16 VARBYTES
) TAGS (
tag1 BOOL,tag2 SMALLINT,tag3 INT,tag4 BIGINT,tag5 FLOAT4,tag6 DOUBLE,tag7 VARBYTES,tag11 CHAR,tag13 NCHAR NOT NULL
)PRIMARY TAGS(tag13);
DROP TABLE tsdb.t2;

-- decommission: c6
-- sleep: 15s
SELECT node_id,decommissioning AS status FROM kwdb_internal.gossip_liveness ORDER BY node_id;
select * from tsdb.t1 order by ts;
SELECT count(*) FROM tsdb.t1;
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;

CREATE TABLE tsdb.t2(
                        ts TIMESTAMPTZ NOT NULL,e1 TIMESTAMP,e2 INT2,e3 INT4,e4 INT8,e5 FLOAT4,e6 FLOAT8,e7 BOOL,e8 CHAR,e10 NCHAR,e16 VARBYTES
) TAGS (
tag1 BOOL,tag2 SMALLINT,tag3 INT,tag4 BIGINT,tag5 FLOAT4,tag6 DOUBLE,tag7 VARBYTES,tag11 CHAR,tag13 NCHAR NOT NULL
)PRIMARY TAGS(tag13);
DROP TABLE tsdb.t2;

-- join: c7
-- sleep: 5s
-- join: c8
-- wait-running: c1
-- sleep: 5s
-- wait-running: c1
SELECT node_id,decommissioning AS status FROM kwdb_internal.gossip_liveness ORDER BY node_id;
select * from tsdb.t1 order by ts;
SELECT count(*) FROM tsdb.t1;
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;


CREATE TABLE tsdb.t2(
                        ts TIMESTAMPTZ NOT NULL,e1 TIMESTAMP,e2 INT2,e3 INT4,e4 INT8,e5 FLOAT4,e6 FLOAT8,e7 BOOL,e8 CHAR,e10 NCHAR,e16 VARBYTES
) TAGS (
tag1 BOOL,tag2 SMALLINT,tag3 INT,tag4 BIGINT,tag5 FLOAT4,tag6 DOUBLE,tag7 VARBYTES,tag11 CHAR,tag13 NCHAR NOT NULL
)PRIMARY TAGS(tag13);
DROP TABLE tsdb.t2;

-- decommission: c7
-- sleep: 5s
SELECT node_id,decommissioning AS status FROM kwdb_internal.gossip_liveness ORDER BY node_id;
select * from tsdb.t1 order by ts;
SELECT count(*) FROM tsdb.t1;
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;

-- DROP TABLE tsdb.t2;
DROP TABLE tsdb.t1;
DROP DATABASE tsdb;
DROP DATABASE tsdb1;

-- decommission: c8
-- sleep: 5s
SELECT node_id,decommissioning AS status FROM kwdb_internal.gossip_liveness ORDER BY node_id;
select * from tsdb.t1 order by ts;
SELECT count(*) FROM tsdb.t1;
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;

-- sleep: 10s
-- kill: c6
-- kill: c7
-- kill: c8

