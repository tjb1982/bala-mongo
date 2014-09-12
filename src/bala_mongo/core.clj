(ns bala-mongo.core
  (:require [marshal.core :as m])
  (:import (org.apache.commons.lang3 ArrayUtils))
  (:gen-class))

(def sep "|||")

(def file-name
  (str "bala-mongo-output "
       (-> (java.util.Date.) .toString)
       ".csv"))

(defprotocol ByteFormat
  (as-hex [bytez]))

(extend-protocol ByteFormat
  
  (Class/forName "[B")
  (as-hex [bytez]
    (apply str (for [b bytez] (format "%02x " b))))
  
  java.io.ByteArrayOutputStream
  (as-hex [bytez]
    (as-hex (.toByteArray bytez)))

  java.nio.ByteBuffer
  (as-hex [bytez]
    (as-hex (.array bytez))))

(defn lil-byte-buffer
  [byte-arr]
  (-> (java.nio.ByteBuffer/wrap byte-arr)
    (.order java.nio.ByteOrder/LITTLE_ENDIAN)))

(defn get-c-string
  ([bb] (get-c-string bb ""))
  ([bb string]
    (when (.hasRemaining bb)
      (let [b (.get bb)]
        (if (not= b 0)
          (get-c-string bb (str string (char b)))
	  string)))))

(defn parse-header
  [bb]
  (if (.hasRemaining bb)
    {:message-length (.getInt bb)
     :request-id (.getInt bb)
     :response-to (.getInt bb)
     :op-code (.getInt bb)}))

(defn parse-documents
  ([bb] (parse-documents bb -1))
  ([bb remaining]
    (parse-documents bb remaining nil))
  ([bb remaining documents]
    (if (or (> remaining 0)
            (and (< remaining 0)
                 (.hasRemaining bb)))
      (let [doc-length (.getInt bb)]
        (.position bb (- (.position bb) 4))
        (let [documents (conj documents
                          (let [ba (byte-array doc-length)]
                            (.get bb ba 0 doc-length)
                            (com.mongodb.util.JSON/serialize
			      (org.bson.BSON/decode ba))))]
	  (parse-documents bb (dec remaining) documents)))
      documents)))

(defn parse-reply
  [bb]
  (let [body {:response-flags (.getInt bb)
              :cursor-id (.getLong bb)
              :starting-from (.getInt bb)
              :number-returned (.getInt bb)}]
    (assoc body :documents
      (parse-documents bb (-> body :number-returned)))))

(defn parse-query
  [bb]
  (let [body {:flags (.getInt bb)
              :full-collection-name (get-c-string bb)
              :number-to-skip (.getInt bb)
              :number-to-return (.getInt bb)
	      :query (parse-documents bb)}]
    body))

(defn parse-insert
  [bb]
  (let [body {:flags (.getInt bb)
              :full-collection-name (get-c-string bb)
              :documents (parse-documents bb)}]
    body))

(defn parse-msg
  [msg]
  (let [bb (lil-byte-buffer (.toByteArray msg))
        header (parse-header bb)]
    (if (.hasRemaining bb)
      {:header header
       :body (condp = (-> header :op-code)
               1 (parse-reply bb)
               2002 (parse-insert bb)
               2004 (parse-query bb)
               nil)})))

(defn report-responses
  [pkg]
  (doall
    (map-indexed
      (fn [i resp]
        (with-open [w (clojure.java.io/writer
                        file-name :append true)]
          (.write w
	    (str
	      (clojure.string/join sep
                [(-> resp :server :name)
                 (str (-> resp :server :host) ":" (-> resp :server :port))
                 (as-hex (-> pkg :request))
                 (parse-msg (-> pkg :request))
                 (as-hex (-> resp :buffer))
                 (parse-msg (-> resp :buffer))])
            "\r\n"))))
      (-> pkg :responses))))

(defn read-wire
  [in]
  (let [header-bytes (byte-array 16)
        first-read (.read in header-bytes 0 16)]
    (if (not= -1 first-read)
      (let [leftover (- (-> (lil-byte-buffer header-bytes)
                          .getInt)
                        16)
            leftover-bytes (byte-array leftover)]
        (if-let [second-read (not= -1 (.read in leftover-bytes 0 leftover))]
          (ArrayUtils/addAll header-bytes leftover-bytes)
          -1))
      first-read)))

(defn before-create-server
  [props]
  (with-open [w (clojure.java.io/writer
                  file-name)]
    (.write w
      (str (clojure.string/join sep
        ["Server Name"
         "Address"
         "Raw Request"
         "Request"
         "Raw Response"
         "Response"])
      "\r\n"))))

(defn read-request
  [in]
  (read-wire in))

(defn read-response
  [in]
  (read-wire in))

(defn handle-interchange
  [pkg]
  (report-responses pkg))

