package com.github.solrqueue.solrbenchmark;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

public class WikiAbstractDocGenerator {
  private static Logger logger = LoggerFactory.getLogger(WikiAbstractDocGenerator.class);
  private static final Map<String, String> ElementMap;
  private SAXParser parser;
  private InputStream inputStream;
  private DocumentIndexer indexer;
  private int nDocs;

  static {
    Map<String, String> aMap = new HashMap<>();
    aMap.put("title", "title_t");
    aMap.put("abstract", "abstract_t");
    aMap.put("url", "url_s");
    ElementMap = Collections.unmodifiableMap(aMap);
  }

  public WikiAbstractDocGenerator(String filename, DocumentIndexer indexer, int nDocs) {
    logger.debug("loading from {}", filename);
    this.indexer = indexer;
    this.nDocs = nDocs;
    try {
      inputStream = new GZIPInputStream(new FileInputStream(filename));
      SAXParserFactory pf = SAXParserFactory.newInstance();
      pf.setNamespaceAware(false);
      pf.setValidating(false);
      parser = pf.newSAXParser();
    } catch (Exception e) {
      throw new RuntimeException("Failed to open file", e);
    }
  }

  public void parse() {
    try {
      parser.parse(inputStream, new WikiAbstractHandler(indexer, nDocs));
    } catch (FinishedParseException fpe) {
      // We consumed all the docs we wanted
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse file", e);
    }
  }

  static class FinishedParseException extends RuntimeException {
    FinishedParseException() {
      super("done");
    }
  }

  static class WikiAbstractHandler extends DefaultHandler {
    private String currentElement;
    private SolrInputDocument doc = null;
    private int count = 0;
    private DocumentIndexer indexer;
    private int nDocs;

    WikiAbstractHandler(DocumentIndexer indexer, int nDocs) {
      this.indexer = indexer;
      this.nDocs = nDocs;
      currentElement = null;
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
      if (qName.equals("doc") && doc != null) {
        logger.debug("doc {}", doc);
        Stats.Produced.inc();
        indexer.update(doc);
      }

      currentElement = null;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
      currentElement = qName;
      if (currentElement.equals("doc")) {
        if (nDocs >= 0 && count > nDocs) {
          throw new FinishedParseException();
        }
        doc = new SolrInputDocument();
        doc.setField("id", "wiki_abstract_" + count);
        count++;
      }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
      if (currentElement == null) return;
      String mapping = ElementMap.get(currentElement);
      if (mapping == null) return;
      doc.setField(mapping, new String(ch, start, length));
    }
  }
}
