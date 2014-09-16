(ns bala-mongo.core
  (:require [marshal.core :as m])
  (:import (org.apache.commons.lang3 ArrayUtils))
  (:gen-class))

(def sep "|||")
(def ^:dynamic props {})
(def time-when-module-was-loaded
     (-> (java.util.Date.)
       .toString
       (clojure.string/replace #"[\s\:]+" "-")))

(defn file-name
  []
  (str (-> props :bala-mongo :output)
       "bala-mongo-output-"
       time-when-module-was-loaded
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
    (as-hex (.array bytez)))

  nil
  (as-hex [bytez] ""))

(defn lil-byte-buffer
  [byte-arr]
  (-> (java.nio.ByteBuffer/wrap byte-arr)
    (.order java.nio.ByteOrder/LITTLE_ENDIAN)))

(defn get-c-string
  ([bb] (get-c-string bb ""))
  ([bb string]
    (when (.hasRemaining bb) ;; i.e., this should return before this is possible if this is actually a \0 terminated string
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
        (if (> doc-length 0)
          (let [documents (conj documents
                            (let [ba (byte-array doc-length)]
                              (.get bb ba 0 doc-length)
                              (try
                                (org.bson.BSON/decode ba)
                                (catch IllegalArgumentException e
                                  (.getMessage e)))))]
	  (parse-documents bb (dec remaining) documents))
          documents))
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
                        (file-name) :append true)]
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

(defn before-connection
  [props]
  (alter-var-root #'props (fn [_] props)) 
  (with-open [w (clojure.java.io/writer
                  (file-name))]
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
  (let [ba (read-wire in)]
    (when (and (not= ba -1)
               (-> props :bala-mongo :max-wire-version)
               (= (second ba) 0x00)) ;; i.e., msglen less than 256
      (let [string (String. ba "ISO-8859-1")
            index (.indexOf string "maxWireVersion")
            wire-version (-> props :bala-mongo :max-wire-version)]
        (if (pos? index)
          (aset-byte ba (+ index 15) wire-version))))
    ba))

(defn expect-response?
  [buf]
  (let [msg (parse-msg buf)]
    (if (some #(= (-> msg :header :op-code) %) [2004 2005])
      true
      false)))

(defn handle-interchange
  [pkg]
  (report-responses pkg))

