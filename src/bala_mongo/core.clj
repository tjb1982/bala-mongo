(ns bala-mongo.core
  (:require [marshal.core :as m])
  (:gen-class))

(defn as-bytes
  [buf]
  (apply str (for [b (.toByteArray buf)] (format "%02x " b))))

(def msg-header (m/struct :message-length m/sint32
                          :request-id m/sint32
                          :response-to m/sint32
                          :op-code m/sint32))

;(def document-t (m/struct :document-length

(def op-reply (m/struct :header msg-header
                        :response-flags m/sint32
                        :cursor-id m/sint64
                        :starting-from m/sint32
                        :number-returned m/sint32
                        :first-document-length m/sint32))

(def op-query (m/struct :header msg-header
                        :flags m/sint32
                        :full-collection-name m/ascii-string
                        :number-to-skip m/sint32
                        :number-to-return m/sint32
                        :query-document-length m/sint32))

(defn parse-message
  [buf & more]
  (let [header (m/read (java.io.ByteArrayInputStream.
			 (.toByteArray buf) 0 16)
		       msg-header)]
;    (try 
    (m/read (java.io.ByteArrayInputStream. (.toByteArray buf))
      (condp = (-> header :op-code)
        1 op-reply
        1000 "OP_MSG"
        2001 "OP_UPDATE"
        2002 "OP_INSERT"
        2003 "RESERVED"
        2004 op-query
        2005 "OP_GET_MORE"
        2006 "OP_DELETE"
        2007 "OP_KILL_CURSORS"))
;       (catch Exception e {:e (.getMessage e)}))
    ))

(defn report-message
  [pkg]
  (let [sep "|||"]
    (with-open [f (clojure.java.io/writer "bala-mongo-output.csv" :append true)]
      (doseq [response (-> pkg :responses)]
        (println (parse-message (-> pkg :request))
                 (parse-message (first response))
                 "\n")
      ))))

(defn handle-messages
  [pkg]
  (report-message pkg))

