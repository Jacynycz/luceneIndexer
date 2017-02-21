/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 *
 * @author usuario
 */
public class Indexer {

	 Directory dir;
	 IndexReader indexReader;
	 QueryParser queryParser;
	 Query q;
	 IndexSearcher indexSearcher;
	 Analyzer analyzer;
	 IndexWriterConfig indexWriterConfig;
	 IndexWriter indexWriter;
	 IndexerMode mode;
	 private String[] fieldNames = null;
	 private boolean[] searchable = null;

	 public Indexer(IndexerMode mode, File pathToDirectory) throws IndexerException {
			this.mode = mode;
			switch (mode) {
				 case READ:
						try {
							 if (!pathToDirectory.exists() && !pathToDirectory.isDirectory()) {
									throw new IndexerException("File not found. PATH: " + pathToDirectory.toPath().toString());
							 }
							 dir = new SimpleFSDirectory(pathToDirectory.toPath());
						} catch (IOException e) {
							 throw new IndexerException(e.getMessage());
						}
						break;
				 case READWRITE:
						try {
							 if (!pathToDirectory.exists() && !pathToDirectory.isDirectory()) {
									throw new IndexerException("File not found. PATH: " + pathToDirectory.toPath().toString());
							 }
							 dir = new SimpleFSDirectory(pathToDirectory.toPath());
						} catch (IOException e) {
							 throw new IndexerException(e.getMessage());
						}
						break;
				 case WRITE:
						try {
							 if (pathToDirectory.exists()) {
									if (!pathToDirectory.isDirectory()) {
										 throw new IndexerException("Specified path is not a directory. PATH: " + pathToDirectory.toPath().toString());
									}
							 } else {
									pathToDirectory.mkdirs();
							 }
							 dir = new SimpleFSDirectory(pathToDirectory.toPath());

						} catch (IOException e) {
							 throw new IndexerException(e.getMessage());
						}
						break;
			}
	 }

	 public IndexReader getReader() {
			if (mode == IndexerMode.READ) {
				 return indexReader;
			} else {
				 return null;

			}
	 }

	 public void open(String fieldToRead, boolean wildcard) throws IndexerException {
			switch (mode) {
				 case READ:
						try {
							 indexReader = DirectoryReader.open(dir);
							 indexSearcher = new IndexSearcher(indexReader);
							 analyzer = new KeywordAnalyzer();
							 queryParser = new QueryParser(fieldToRead, analyzer);
							 queryParser.setAllowLeadingWildcard(wildcard);
						} catch (IOException ex) {
							 throw new IndexerException(ex.getMessage());
						}
						break;
				 case READWRITE:
						try {
							 indexReader = DirectoryReader.open(dir);
							 indexSearcher = new IndexSearcher(indexReader);
							 indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
							 indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
							 indexWriter = new IndexWriter(dir, indexWriterConfig);
							 analyzer = new KeywordAnalyzer();
							 queryParser = new QueryParser(fieldToRead, analyzer);
							 queryParser.setAllowLeadingWildcard(wildcard);
						} catch (IOException ex) {
							 throw new IndexerException(ex.getMessage());
						}
						break;
				 case WRITE:
						throw new IndexerException("El índice está en modo escritura. Utliza el método open()");
			}
	 }

	 public void open() throws IndexerException {
			if (mode == IndexerMode.WRITE) {
				 try {
						indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
						indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
						indexWriter = new IndexWriter(dir, indexWriterConfig);
				 } catch (IOException ex) {
						throw new IndexerException(ex.getMessage());
				 }
			} else {
				 throw new IndexerException("El índice está en modo lectura. Utliza el método open(String fieldToRead, boolean wildcard)");
			}
	 }

	 public ArrayList<Document> read(String query) throws IndexerException {
			ArrayList<Document> docs = null;
			if (mode == IndexerMode.READ || mode == IndexerMode.READWRITE) {
				 if (queryParser != null) {
						try {
							 q = queryParser.parse(query);
							 TopDocs topDocs = indexSearcher.search(q, 2);
							 ScoreDoc[] scoreDocs = topDocs.scoreDocs;
							 docs = new ArrayList<>();
							 for (ScoreDoc doc : scoreDocs) {
									docs.add(indexSearcher.doc(doc.doc));
							 }

						} catch (ParseException | IOException ex) {
							 throw new IndexerException("Error en la query de búsqueda "+query);
						}
				 } else {
						throw new IndexerException("Antes de poder leer, utiliza el método open(String fieldToRead, boolean wildcard)");
				 }
			} else {
				 throw new IndexerException("El índice está en modo escritura.");
			}
			return docs;
	 }

	 public void setFieldNames(String[] fieldNames, boolean[] searchable) {
			this.fieldNames = fieldNames;
			this.searchable = searchable;
	 }

	 public void write(String[] values) throws IndexerException {
			if (fieldNames == null || searchable == null) {
				 throw new IndexerException("No se ha establecido el nombre de los campos, utiliza setFieldNames(String[] fieldNames)");
			}
			if (fieldNames.length != values.length || searchable.length != values.length) {
				 throw new IndexerException("Los valores de los campos de entrada tienen tamaños distintos");
			}
			try {
				 Document doc = new Document();
				 for (int i = 0; i < fieldNames.length; i++) {
						if (searchable[i]) {
							 doc.add(new TextField(fieldNames[i], values[i], Field.Store.YES));
						} else {
							 doc.add(new StringField(fieldNames[i], values[i], Field.Store.YES));
						}
				 }
				 indexWriter.addDocument(doc);
				 indexWriter.commit();
			} catch (Exception e) {
				 throw new IndexerException("Error al añadir el archivo al índice");
			}
	 }

	 public void close() {
			if (indexReader != null) {
				 try {
						indexReader.close();
				 } catch (IOException ex) {
						Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
				 }
			}
			if (indexWriter != null) {
				 try {
						indexWriter.close();
				 } catch (IOException ex) {
						Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
				 }
			}
	 }

}
