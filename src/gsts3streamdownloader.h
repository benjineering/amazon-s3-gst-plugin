// benjineering 👍
#ifndef __GST_S3_STREAM_DOWNLOADER_H__
#define __GST_S3_STREAM_DOWNLOADER_H__

#include "gsts3downloader.h"

G_BEGIN_DECLS

GST_DEBUG_CATEGORY_EXTERN(gst_s3_sink_debug);

typedef struct _GstS3StreamDownloader GstS3StreamDownloader;

GstS3Downloader * gst_s3_stream_downloader_new (const GstS3Config * config);

G_END_DECLS

#endif /* __GST_S3_STREAM_DOWNLOADER_H__ */
