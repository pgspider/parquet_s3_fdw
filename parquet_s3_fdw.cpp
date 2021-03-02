/*-------------------------------------------------------------------------
 *
 * parquet_s3_fdw.cpp
 *		  S3 accessing module for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/parquet_s3_fdw.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "parquet_s3_fdw.hpp"

#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

using namespace std;

#define S3_ALLOCATION_TAG "S3_ALLOCATION_TAG"

/* Implementation of S3RandomAccessFile class methods */
S3RandomAccessFile::S3RandomAccessFile(Aws::S3::S3Client *s3_client,
				   const Aws::String &bucket, const Aws::String &object)
				: bucket_(bucket), object_(object), s3_client_(s3_client) {
	offset = 0;
	isclosed = false;
}

arrow::Status
S3RandomAccessFile::Close() 
{
	isclosed = true;
	return arrow::Status::OK();
}

arrow::Result<int64_t>
S3RandomAccessFile::Tell() const
{
	return offset;
}

bool
S3RandomAccessFile::closed() const
{
	return isclosed;
}

arrow::Status
S3RandomAccessFile::Seek(int64_t position)
{
	offset = position;
	return arrow::Status::OK();
}

arrow::Result<int64_t>
S3RandomAccessFile::Read(int64_t nbytes, void* out)
{
	Aws::S3::Model::GetObjectRequest object_request;
	object_request.WithBucket(bucket_.c_str()).WithKey(object_.c_str());
	string bytes = "bytes=" + to_string(offset) + "-" + to_string(offset + nbytes - 1);
	object_request.SetRange(bytes.c_str());
	object_request.SetBucket(this->bucket_);
	object_request.SetKey(this->object_);
#if 0
	object_request.SetResponseStreamFactory([](){
		return Aws::New<Aws::FStream>(
			"ALLOCATION_TAG", "DOWNLOADED_FILENAME", std::ios_base::out); });
#else
	object_request.SetResponseStreamFactory([](){
		return Aws::New<Aws::StringStream >(S3_ALLOCATION_TAG); });
#endif

	Aws::S3::Model::GetObjectOutcome get_object_outcome = this->s3_client_->GetObject(object_request);
	if (!get_object_outcome.IsSuccess()) {
        auto err = get_object_outcome.GetError();
        Aws::String msg = "GetObject failed. " + err.GetExceptionName() + ": " + err.GetMessage();
		return arrow::Status(arrow::StatusCode::IOError, msg.c_str());
	}

	int64_t n_read = get_object_outcome.GetResult().GetContentLength();
    offset += n_read;
	std::stringstream string_stream;
	string_stream << get_object_outcome.GetResult().GetBody().rdbuf();
	string_stream.read((char*)out, n_read);
	return n_read;
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
S3RandomAccessFile::Read(int64_t nbytes)
{
	char *out = (char*)malloc(nbytes);
	arrow::Result<int64_t> res = this->Read(nbytes, out);
	int64_t n = res.ValueOrDie();
	std::shared_ptr<arrow::Buffer> buf = make_shared<arrow::Buffer>((const uint8_t*)out, n);
	return buf;
}

arrow::Result<int64_t>
S3RandomAccessFile::GetSize()
{
	Aws::S3::Model::HeadObjectRequest headObj;
	headObj.SetBucket(bucket_);
	headObj.SetKey(object_);
	auto object = this->s3_client_->HeadObject(headObj);
	if (!object.IsSuccess())
	{
		return arrow::Status(arrow::StatusCode::IOError, "HeadObject failed");
	}

	int64_t fileSize = object.GetResultWithOwnership().GetContentLength();
	return fileSize;
}
