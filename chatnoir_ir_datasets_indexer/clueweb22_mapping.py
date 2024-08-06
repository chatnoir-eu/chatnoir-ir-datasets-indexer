
class _ClueWeb22DataRecord(DataRecord):
    warc_target_uri_hash: str


class _ClueWeb22MetaRecord(PlaintextMetaRecord):
    warc_target_uri_hash: str


class ClueWeb22Mapping(
    DatasetMapping[GenericDoc, _ClueWeb22MetaRecord, _ClueWeb22DataRecord]
):
    num_data_shards = 40
    num_data_replicas = 1
    num_meta_shards = 10
    num_meta_replicas = 1
    base_dir = _IR_DATASETS_HOME / "clueweb22" / "corpus"
    corpus_prefix = "clueweb22"

    def record_time(self, doc: GenericDoc) -> datetime:
        return doc.date

    @staticmethod
    def _doc_id(doc: GenericDoc):
        """Parse document ID into components."""
        return ClueWeb22DocId.from_string(doc.doc_id)

    def _path(self, doc: GenericDoc, format_type) -> Path:
        name = f"{self._doc_id(doc).path}{format_type.extension}"
        return self.base_dir / format_type.id / name

    def _offset(self, doc: GenericDoc, format_type) -> int:
        doc_id = self._doc_id(doc)
        # Determine offset path.
        offsets_name = f"{doc_id.path}{format_type.offset_extension}"
        offsets_path = self.base_dir / format_type.id / offsets_name
        # Read offsets.
        with offsets_path.open("rt", encoding="utf8") as offsets_lines:
            # Seek to the document offset.
            offsets_lines = islice(offsets_lines, doc_id.doc, doc_id.doc + 1)
            # Determine current document offset.
            return int(next(offsets_lines))

    def warc_path(self, doc) -> Path:
        return self._path(doc, ClueWeb22Format.HTML)

    def warc_offset(self, doc) -> int:
        return self._offset(doc, ClueWeb22Format.HTML)

    def plaintext_path(self, doc) -> Path:
        return self._path(doc, ClueWeb22Format.TXT)

    def plaintext_file(self, doc: _DocumentType, s3_bucket: str) -> str:
        relative_path = self.plaintext_path(doc).relative_to(self.base_dir)
        return f"s3://{s3_bucket}/{relative_path}"

    def plaintext_offset(self, doc) -> int:
        return self._offset(doc, ClueWeb22Format.TXT)

    def meta_record(
            self,
            doc,
            s3_bucket: str,
    ) -> Optional[_ClueWeb22MetaRecord]:
        return _ClueWeb22MetaRecord(
            uuid=self.webis_id(doc),
            source_file=self.warc_file(doc, s3_bucket),
            source_offset=self.warc_offset(doc),
            warc_type="resource",
            warc_target_uri=doc.url,
            warc_target_uri_hash=doc.url_hash,
            warc_warcinfo_id=None,
            warc_date=doc.date,
            warc_record_id=str(doc.record_id),
            warc_trec_id=doc.doc_id,
            warc_identified_payload_type="text/html",
            warc_payload_digest=doc.payload_digest,
            warc_block_digest=None,
            warc_ip_address=None,
            content_type="text/html",
            content_length=len(doc.html),
            http_date=doc.date,
            http_content_type="text/html",
            http_content_length=len(doc.html),
            content_encoding="utf8",
            plaintext_source_file=self.plaintext_file(doc, s3_bucket),
            plaintext_source_offset=self.plaintext_offset(doc),
            plaintext_content_type="application/x-ndjson+clueweb22",
        )

    def data_record(
            self,
            doc,
    ) -> Optional[_ClueWeb22DataRecord]:
        html_tree = HTMLTree.parse_from_bytes(doc.html, "utf8")
        if not html_tree.body:
            LOGGER.info(f"Skipping document {doc.doc_id}: No body.")
            return None

        content_full = extract_plain_text(
            html_tree,
            alt_texts=True,
            preserve_formatting=False,
        )
        if len(content_full) <= 0:
            LOGGER.info(
                f"Skipping document {doc.doc_id}: "
                f"Document empty after full content extraction."
            )
            return None

        replacement_count = content_full.count(_REPLACEMENT_CHAR)
        if replacement_count / len(content_full) > 0.1:
            LOGGER.info(
                f"Skipping document {doc.doc_id}: "
                f"Document contains more than 10% Unicode "
                f"replacement characters."
            )
            return None
        if replacement_count > 0:
            content_full = content_full.replace(_REPLACEMENT_CHAR, " ")
            content_full = collapse_whitespace(content_full)

        main_content = doc.text
        if len(main_content) < 200:
            LOGGER.info(
                f"Skipping document {doc.doc_id}: "
                f"Main content too short ({len(main_content)} codepoints)."
            )
            return None

        parse_url = urlparse(doc.url)
        title = extract_title(html_tree)
        meta_keywords = extract_meta_keywords(html_tree)
        meta_description = extract_meta_description(html_tree)

        language = doc.language
        if "_" in language:
            language = language.split("_")[0]

        return _ClueWeb22DataRecord(
            uuid=self.webis_id(doc),
            lang=language,
            warc_date=doc.date,
            warc_record_id=str(doc.record_id),
            warc_trec_id=doc.doc_id,
            warc_target_uri=doc.url,
            warc_target_hostname=parse_url.hostname,
            warc_target_path=parse_url.path,
            warc_target_query_string=parse_url.query,
            warc_target_uri_hash=doc.url_hash,
            http_date=doc.date,
            http_content_type="text/html",
            title=title,
            meta_keywords=meta_keywords,
            meta_desc=meta_description,
            body=main_content,
            body_length=len(main_content),
            full_body=content_full,
            headings=extract_headings(html_tree, 3),
        )
