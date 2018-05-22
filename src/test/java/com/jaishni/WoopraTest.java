package com.jaishni;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class WoopraTest {

    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void setUp() {
        Woopra.MyMapper mapper = new Woopra.MyMapper();
        Woopra.MyReducer reducer = new Woopra.MyReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

    }

    // MAPPER TESTS
    @Test
    public void testSingleTimePidMapper() throws IOException {
        String line1 = "{\"continent\":\"NA\",\"date\":\"2017-03-31\",\"country\":\"CA\",\"city\":\"Toronto\",\"timezone\":\"America/Toronto\",\"screen\":\"375x667\",\"description\":\"\",\"language\":\"English\",\"pid\":\"HPz6OQMsgXrH\",\"type\":\"visit\",\"resolution\":\"375x667\",\"second\":59,\"duration\":0,\"number\":0,\"post\":\"M2N\",\"browser\":\"Unknown\",\"id\":\"yMySDAlNQNNa\",\"lat\":43.7673,\"hour_of_day\":23,\"os\":\"iphone\",\"lng\":-79.4111,\"offset\":\"-05:00\",\"method\":\"\",\"org\":\"\",\"ip\":\"70.50.210.176\",\"h\":\"HPz6OQMsgXrH\",\"minute\":59,\"ct\":\"cable_dsl\",\"referrer\":{\"query\":\"\",\"type\":\"direct\",\"url\":\"\"},\"time\":1491029999761,\"region\":\"Ontario\",\"visitor\":{\"company size\":\"N/A\",\"pending quantcast score\":\"0\",\"fname\":\"Reed\",\"first_referrer_query\":\"-encrypted-\",\"agent\":\"false\",\"yearlyvalue\":\"79.95\",\"avatar\":\"http://www.gravatar.com/avatar.php?gravatar_id=53c312d1922ca765c9565e2a724569d7&size=256&default=http%3A%2F%2Fstatic.woopra.com%2Fimages%2Favatar-placeholder.png\",\"version\":\"current\",\"first_visit_time\":\"1486261327578\",\"social_updated_at\":\"1490751142053\",\"lname\":\"Zhao\",\"signup_date\":\"1484542800000\",\"wid\":\"249814\",\"phone\":\"N/A\",\"first_campaign\":\"Signup Confirmation\",\"name\":\"Reed Zhao\",\"company\":\"wesharee\",\"website score\":\"0\",\"first_referrer_url\":\"https://www.google.ca/\",\"email\":\"admin@wesharee.com\",\"first_referrer_type\":\"search\",\"username\":\"reedzhao\"},\"device\":\"mobile\",\"actions\":[{\"duration\":0,\"date\":\"11:59:59 PM\",\"exit\":true,\"system\":false,\"landing\":true,\"domain\":\"woopra.com\",\"name\":\"mobileview\",\"id\":\"yMySDAlNQNNa\",\"time\":1491029999761,\"https\":true,\"passive\":false,\"properties\":{\"website\":\"fangintel.com\",\"package\":\"Small Business 1\",\"name\":\"mobileview\",\"title\":\"Analytics\",\"version\":\"4.2.1\"}}]}";
        mapDriver.withInput(new LongWritable(1L), new Text(line1));
        mapDriver.withOutput(new Text("HPz6OQMsgXrH"), new LongWritable(1491029999761L));
        mapDriver.runTest()
        ;
    }

    @Test
    public void testMultiTimeSinglePidMapper() throws IOException {
        String line1 = "{\"continent\":\"NA\",\"date\":\"2017-04-01\",\"country\":\"US\",\"city\":\"Daly City\",\"timezone\":\"America/Los_Angeles\",\"screen\":\"1920x1080\",\"description\":\"\",\"language\":\"English\",\"pid\":\"xjELVmgxAFho\",\"type\":\"visit\",\"resolution\":\"1920x1080\",\"second\":11,\"duration\":9560,\"number\":1,\"post\":\"94014\",\"browser\":\"Chrome\",\"id\":\"1491067691186\",\"lat\":37.6869,\"hour_of_day\":10,\"os\":\"Linux\",\"lng\":-122.4389,\"offset\":\"-08:00\",\"method\":\"\",\"org\":\"\",\"ip\":\"50.185.8.26\",\"h\":\"xjELVmgxAFho\",\"minute\":28,\"ct\":\"cable_dsl\",\"referrer\":{\"query\":\"\",\"type\":\"direct\",\"url\":\"\"},\"time\":1491067691186,\"region\":\"California\",\"visitor\":{\"company size\":\"N/A\",\"sion\":\"current\",\"rname\":\"jad\",\"pany size\":\"N/A\",\"pid\":\"xjELVmgxAFho\",\"linkedin_username\":\"jad-younan-9587309\",\"twitter_handle\":\"balanko\",\"signup_date\":\"1207800000000\",\"twitter\":\"25062674\",\"social_avatar_source\":\"Gravatar\",\"me\":\"Younan\",\"id\":\"1558540\",\"twitter_id\":\"25062674\",\"b.ip\":\"184.74.195.58\",\"pending quantcast score\":\"0\",\"fname\":\"Jad\",\"first_referrer_query\":\"-encrypted-\",\"il\":\"jad.younan@gmail.com\",\"tar\":\"http://www.gravatar.com/avatar.php?gravatar_id=6353484a05ba15c08bfe707c212a83bd&size\",\"nup_date\":\"1207800000000\",\"ip\":\"24.4.141.79\",\"yearlyvalue\":\"1.00\",\"household_income\":\"150k-175k\",\"version\":\"current\",\"phone\":\"2407455252\",\"ip address\":\"24.4.141.79\",\"name\":\"Jad Younan\",\"support\":\"Yes\",\"first_referrer_type\":\"direct\",\"company_size\":\"N/A\",\"social_name\":\"Jad Younan\",\"gender\":\"male\",\"city\":\"Daly City\",\"enterprise\":\"true\",\"aweraewr\":\"ewraewrwerwer\",\"pany\":\"Ifusion labs\",\"admin\":\"1\",\"linkedin\":\"jad-younan/9/730/958\",\"chat_message\":\"true\",\"first_visit_time\":\"1490225433241\",\"platform\":\"Mac\",\"lname\":\"Younan\",\"linkedin_bio\":\"Application developer, system engineer. Designs and builds scalable-reliable backend applications and services. Woopra: The platform is designed to help organizations optimize the entire customer lifecycle by delivering live, granular behavioral data for individual website visitors and customers. It ties this individual-level data to aggregate analytics reports for a full lifecycle view that bridges departmental gaps. Woopra tracks over 200,000 websites, 15 Billion actions per month and over half a million visitors per minute. Specialties: Identifying emerging technology trends, and delivering best-in-class solutions for the industry, Cloud computing, Massive parallel processing, Business intelligence, Statistics, Data mining, Full software stack, SaaS marketplace, Novel data systems, System architecture.\",\"desktop\":\"South San Francisco\",\"wid\":\"18\",\"company\":\"Ifusion labs\",\"website score\":\"0\",\"first_referrer_url\":\"https://woopra.zendesk.com/agent/\",\"monthly total\":\"55\",\"email\":\"jad.younan@gmail.com\",\"rlyvalue\":\"1.00\",\"website\":\"blabla.com\",\"d\":\"woopra.com\",\"e\":\"Jad Younan\",\"facebook\":\"703655572\",\"h\":\"xjELVmgxAFho\",\"linkedin_id\":\"28679252\",\"social_avatar\":\"https://d2ojpxxtu63wzl.cloudfront.net/static/d0d257b7f00821a0657734a1a17f65a0_02dd840a1f41681d330a2758530c8b5cec7fc056319dd81e7799ea99c4e21b10\",\"avatar_source\":\"Gravatar\",\"avatar\":\"http://www.gravatar.com/avatar.php?gravatar_id=6353484a05ba15c08bfe707c212a83bd&size=256&default=http%3A%2F%2Fstatic.woopra.com%2Fimages%2Favatar-placeholder.png\",\"woopra_social_updated_at\":\"1475817672745\",\"url\":\"/live/live/dashboard\",\"social_updated_at\":\"1490658874222\",\"first_campaign\":\"blog01162014\",\"${visit.ip}\":\"24.4.141.79\",\"_mkt_trk\":\"null\",\"age\":\"35-44\",\"username\":\"jad\"},\"device\":\"desktop\",\"actions\":[{\"duration\":0,\"date\":\"7:53:39 PM\",\"exit\":true,\"system\":false,\"domain\":\"woopra.com\",\"name\":\"app-object\",\"id\":\"sbgSHyijRer2\",\"time\":1491101619018,\"passive\":false,\"properties\":{\"website\":\"jadyounan.com\",\"name\":\"app-object\",\"action\":\"delete\",\"type\":\"widgets\",\"value\":\"now\"}},{\"duration\":2,\"date\":\"7:53:36 PM\",\"system\":false,\"domain\":\"woopra.com\",\"name\":\"web-view\",\"id\":\"EAZ140YvnX0a\",\"time\":1491101616981,\"passive\":false,\"properties\":{\"website\":\"jadyounan.com\",\"package\":\"VIP\",\"name\":\"web-view\",\"title\":\"Dashboard\",\"version\":\"11.4.10\",\"url\":\"/live/dashboard\"}},{\"duration\":4,\"date\":\"7:53:32 PM\",\"system\":false,\"domain\":\"woopra.com\",\"name\":\"web-view\",\"id\":\"UOBlAHxMudvG\",\"time\":1491101612400,\"passive\":false,\"properties\":{\"website\":\"jadyounan.com\",\"package\":\"VIP\",\"name\":\"web-view\",\"title\":\"People\",\"version\":\"11.4.10\",\"url\":\"/live/people/online\"}},{\"duration\":0,\"date\":\"7:53:31 PM\",\"system\":false,\"domain\":\"woopra.com\",\"name\":\"request report\",\"id\":\"SlpAlZ8NZLIl\",\"time\":1491101611829,\"passive\":false,\"properties\":{\"duration\":\"6602\",\"website\":\"jadyounan.com\",\"endpoint\":\"retention\",\"requestid\":\"KGE310\",\"name\":\"request report\",\"status\":\"200\"}},{\"duration\":3,\"date\":\"7:53:28 PM\",\"system\":false,\"domain\":\"woopra.com\",\"name\":\"web-view\",\"id\":\"3vOEPSfNi3Xe\",\"time\":1491101608429,\"passive\":false,\"properties\":{\"website\":\"jadyounan.com\",\"package\":\"VIP\",\"name\":\"web-view\",\"title\":\"Dashboard\",\"version\":\"11.4.10\",\"url\":\"/live/dashboard\"}}]}";
        mapDriver.withInput(new LongWritable(1L), new Text(line1));
        mapDriver.withOutput(new Text("xjELVmgxAFho"), new LongWritable(1491101619018L));
        mapDriver.runTest();


    }

    @Test
    public void testMultiTimeMultiPidMapper() throws IOException {
        final String multiLineInput = "{\"continent\":\"NA\",\"date\":\"2017-03-31\",\"country\":\"CA\",\"city\":\"Toronto\",\"timezone\":\"America/Toronto\",\"screen\":\"375x667\",\"description\":\"\",\"language\":\"English\",\"pid\":\"HPz6OQMsgXrH\",\"type\":\"visit\",\"resolution\":\"375x667\",\"second\":59,\"duration\":0,\"number\":0,\"post\":\"M2N\",\"browser\":\"Unknown\",\"id\":\"yMySDAlNQNNa\",\"lat\":43.7673,\"hour_of_day\":23,\"os\":\"iphone\",\"lng\":-79.4111,\"offset\":\"-05:00\",\"method\":\"\",\"org\":\"\",\"ip\":\"70.50.210.176\",\"h\":\"HPz6OQMsgXrH\",\"minute\":59,\"ct\":\"cable_dsl\",\"referrer\":{\"query\":\"\",\"type\":\"direct\",\"url\":\"\"},\"time\":1491029999761,\"region\":\"Ontario\",\"visitor\":{\"company size\":\"N/A\",\"pending quantcast score\":\"0\",\"fname\":\"Reed\",\"first_referrer_query\":\"-encrypted-\",\"agent\":\"false\",\"yearlyvalue\":\"79.95\",\"avatar\":\"http://www.gravatar.com/avatar.php?gravatar_id=53c312d1922ca765c9565e2a724569d7&size=256&default=http%3A%2F%2Fstatic.woopra.com%2Fimages%2Favatar-placeholder.png\",\"version\":\"current\",\"first_visit_time\":\"1486261327578\",\"social_updated_at\":\"1490751142053\",\"lname\":\"Zhao\",\"signup_date\":\"1484542800000\",\"wid\":\"249814\",\"phone\":\"N/A\",\"first_campaign\":\"Signup Confirmation\",\"name\":\"Reed Zhao\",\"company\":\"wesharee\",\"website score\":\"0\",\"first_referrer_url\":\"https://www.google.ca/\",\"email\":\"admin@wesharee.com\",\"first_referrer_type\":\"search\",\"username\":\"reedzhao\"},\"device\":\"mobile\",\"actions\":[{\"duration\":0,\"date\":\"11:59:59 PM\",\"exit\":true,\"system\":false,\"landing\":true,\"domain\":\"woopra.com\",\"name\":\"mobileview\",\"id\":\"yMySDAlNQNNa\",\"time\":1491029999761,\"https\":true,\"passive\":false,\"properties\":{\"website\":\"fangintel.com\",\"package\":\"Small Business 1\",\"name\":\"mobileview\",\"title\":\"Analytics\",\"version\":\"4.2.1\"}}]}\n" +
                "{\"continent\":\"NA\",\"date\":\"2017-03-31\",\"country\":\"US\",\"city\":\"Trabuco Canyon\",\"timezone\":\"America/Los_Angeles\",\"screen\":\"1280x800\",\"description\":\"\",\"language\":\"English\",\"pid\":\"FRkks83XfcD1\",\"type\":\"visit\",\"resolution\":\"1280x800\",\"second\":26,\"duration\":219,\"number\":2,\"post\":\"92679\",\"browser\":\"Safari 10\",\"id\":\"OF4pMUVQniEa\",\"lat\":33.6117,\"hour_of_day\":23,\"os\":\"Mac\",\"lng\":-117.5491,\"offset\":\"-08:00\",\"method\":\"\",\"org\":\"\",\"ip\":\"70.181.68.7\",\"h\":\"FRkks83XfcD1\",\"minute\":56,\"ct\":\"cable_dsl\",\"referrer\":{\"query\":\"\",\"type\":\"internal\",\"url\":\"https://www.woopra.com/members/\"},\"time\":1491029786188,\"region\":\"California\",\"visitor\":{\"company size\":\"N/A\",\"pid\":\"FRkks83XfcD1\",\"linkedin\":\"-\",\"first_visit_time\":\"1440089878469\",\"lname\":\"McDonald\",\"signup_date\":\"1304568000000\",\"twitter\":\"-\",\"wid\":\"134429\",\"company\":\"alovelydeal.com\",\"website score\":\"0\",\"email\":\"greg@alovelydeal.com\",\"pending quantcast score\":\"0\",\"fname\":\"Greg\",\"ip\":\"104.172.249.176\",\"facebook\":\"-\",\"h\":\"frkks83xfcd1\",\"yearlyvalue\":\"0\",\"avatar\":\"http://www.gravatar.com/avatar.php?gravatar_id=e06b23a4dfaffac8fdaa38315b09e4c4&size=256&default=http%3A%2F%2Fstatic.woopra.com%2Fimages%2Favatar-placeholder.png\",\"woopra_social_updated_at\":\"1477424801460\",\"version\":\"current\",\"social_updated_at\":\"1490657646877\",\"first_campaign\":\"upgrade\",\"name\":\"Greg McDonald\",\"first_referrer_type\":\"direct\",\"username\":\"alovelydeal\"},\"device\":\"desktop\",\"actions\":[{\"duration\":18,\"date\":\"11:59:47 PM\",\"exit\":true,\"system\":false,\"domain\":\"woopra.com\",\"name\":\"request report\",\"id\":\"dkApW03TCgJR\",\"time\":1491029987100,\"https\":true,\"passive\":false,\"properties\":{\"duration\":\"8223\",\"website\":\"gregoryrossblog.com\",\"endpoint\":\"search\",\"requestid\":\"T5KBC3PDGDX4\",\"name\":\"request report\",\"status\":\"200\"}},{\"duration\":200,\"date\":\"11:56:26 PM\",\"system\":false,\"landing\":true,\"domain\":\"woopra.com\",\"name\":\"request report\",\"id\":\"OF4pMUVQniEa\",\"time\":1491029786188,\"https\":true,\"passive\":false,\"properties\":{\"duration\":\"10138\",\"website\":\"gregoryrossblog.com\",\"endpoint\":\"search\",\"requestid\":\"2VVF34A9MOZ6\",\"name\":\"request report\",\"status\":\"200\"}}]}\n";

        Text pid;
        LongWritable timeStamp;

        mapDriver.withInput(new LongWritable(1L), new Text(multiLineInput));

        final List<Pair<Text, LongWritable>> result = mapDriver.run();
        assertEquals(result.size(), 2);

        for (int i = 0; i < result.size(); i++) {
            pid = result.get(i).getFirst();
            timeStamp = result.get(i).getSecond();

            if (pid.equals("HPz6OQMsgXrH")) {
                assertEquals(1491029999761L, timeStamp);
            }

            if (pid.equals("FRkks83XfcD1")) {
                assertEquals(1491029999761L, timeStamp);
            }
        }


    }


    // REDUCER TESTS
    @Test
    public void testMultiTimeMultiPidReducer() throws IOException {

        final List<LongWritable> timeStamp = new ArrayList<LongWritable>();

        timeStamp.add(new LongWritable(1491029999761L));
        timeStamp.add(new LongWritable(1491029999762L));
        timeStamp.add(new LongWritable(1491029999763L));

        reduceDriver.withInput(new Text("frkks83xfcd1"), timeStamp);
        reduceDriver.withOutput(new Text("frkks83xfcd1"), new LongWritable(1491029999763L));
        reduceDriver.runTest();

    }
  }