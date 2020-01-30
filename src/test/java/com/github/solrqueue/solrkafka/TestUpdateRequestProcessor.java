package com.github.solrqueue.solrkafka;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

class TestUpdateRequestProcessor extends UpdateRequestProcessor {
  static class Factory extends UpdateRequestProcessorFactory {
    TestUpdateRequestProcessor lastInstance;

    @Override
    public UpdateRequestProcessor getInstance(
        SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      lastInstance = new TestUpdateRequestProcessor(next);
      return lastInstance;
    }
  }

  TestUpdateRequestProcessor(UpdateRequestProcessor next) {
    super(next);
  }

  TestUpdateRequestProcessor() {
    super(null);
  }

  int processAddCount = 0;
  int processDeleteCount = 0;
  AddUpdateCommand lastAdd;
  DeleteUpdateCommand lastDelete;

  @Override
  public void processAdd(AddUpdateCommand cmd) {
    lastAdd = cmd;
    processAddCount += 1;
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) {
    lastDelete = cmd;
    processDeleteCount += 1;
  }
}
