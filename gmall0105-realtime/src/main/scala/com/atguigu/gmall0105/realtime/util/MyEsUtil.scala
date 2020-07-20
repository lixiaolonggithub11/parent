package com.atguigu.gmall0105.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}

object MyEsUtil {
var factory:JestClientFactory=null;
  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())

  }
  def addDoc():Unit = {
    val jest: JestClient = getClient
    val index = new Index.Builder(Movie0105("2","西游记","孙悟空")).index("movie_0622").`type`("movie").id("2").build()
    val message: String = jest.execute(index).getErrorMessage
    if (message!=null){
      println(message)
    }

    jest.close()
  }
  def bulkDoc(sourceList:List[(String,Any)],indexName:String): Unit = {
    if (sourceList != null && sourceList.size > 0) {
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for ((id,source) <- sourceList) {
        val index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES:" + items.size() + "条数")

      jest.close()

    }
  }

  def queryDoc():Unit ={
    val jest:JestClient = getClient
    val query = "{\n  \"query\": {\n    \"match_phrase\": {\n      \"name\": \"operation red\"\n    }\n  }\n}"
    val search = new Search.Builder(query).addIndex("movie_index0105").build()
    val result: SearchResult = jest.execute(search)
    val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConversions._
    for(hit <- hits){
      println(hit.source.mkString(","))
    }
      jest.close()
  }
  

  def main(args: Array[String]): Unit = {
    addDoc()
    queryDoc()
  }

case class Movie0105(id:String,movie_name:String,name:String)
}
