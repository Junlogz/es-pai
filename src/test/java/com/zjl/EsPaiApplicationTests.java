package com.zjl;

import com.alibaba.fastjson.JSON;
import com.zjl.pojo.User;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsPaiApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    @Test
    void testCreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("zzz");
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("zzz");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void testAddDocument() throws IOException {
        User user = new User("zhangsan", 19);
        IndexRequest request = new IndexRequest("zzz");
        request.id("1");
        request.timeout(TimeValue.timeValueMillis(1000));
        request.source(JSON.toJSONString(user), XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
        System.out.println(response);
    }

    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("zzz", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
        System.out.println(getResponse);
    }

    @Test
    void updateRequest() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("zzz", "1");
        updateRequest.timeout("1s");

        User user = new User("lisi", 11);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(updateResponse.status());
    }

    @Test
    public void testBulk() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> users = new ArrayList<>();
        users.add(new User("zhangsan-1",1));
        users.add(new User("zhangsan-2",2));
        users.add(new User("zhangsan-3",3));
        users.add(new User("zhangsan-4",4));
        users.add(new User("zhangsan-5",5));
        users.add(new User("zhangsan-6",6));

        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("sss")
                            .id("" + (i + 1))
                            .source(JSON.toJSONString(users.get(i)), XContentType.JSON)

            );
        }

        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
    }

    @Test
    public void testSearch() throws IOException {
        // 1.????????????????????????
        SearchRequest searchRequest = new SearchRequest();
        // 2.??????????????????
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // (1)???????????? ??????QueryBuilders???????????????
        // ????????????
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "zhangsan");
        //        // ????????????
        //        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        // (2)??????<????????????>?????????????????? SearchSourceBuilder ??????????????????
        // ????????????
        searchSourceBuilder.highlighter(new HighlightBuilder());
        //        // ??????
        //        searchSourceBuilder.from();
        //        searchSourceBuilder.size();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        // (3)????????????
        searchSourceBuilder.query(termQueryBuilder);
        // 3.?????????????????????
        searchRequest.source(searchSourceBuilder);
        // 4.?????????????????????
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        // 5.??????????????????
        SearchHits hits = search.getHits();
        System.out.println(JSON.toJSONString(hits));
        System.out.println("=======================");
        for (SearchHit documentFields : hits.getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }
    }

}
