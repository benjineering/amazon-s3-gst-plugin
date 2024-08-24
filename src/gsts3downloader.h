// benjineering 👍
#ifndef __GST_S3_DOWNLOADER_H__
#define __GST_S3_DOWNLOADER_H__

#include <glib.h>

#include "gsts3config.h"

G_BEGIN_DECLS

typedef struct _GstS3Downloader GstS3Downloader;

typedef struct
{
  void (*destroy)(GstS3Downloader *);
  gboolean (*download_part)(GstS3Downloader *, const gchar *, gsize);
  gboolean (*complete)(GstS3Downloader *);
} GstS3DownloaderClass;

struct _GstS3Downloader
{
  GstS3DownloaderClass *klass;
};

GstS3Downloader *gst_s3_downloader_new_default(const GstS3Config *config);

void gst_s3_downloader_destroy(GstS3Downloader *downloader);

gboolean gst_s3_downloader_download_part(GstS3Downloader *
                                             downloader,
                                         const gchar *buffer, gsize size);

gboolean gst_s3_downloader_complete(GstS3Downloader *downloader);

G_END_DECLS

#endif /* __GST_S3_DOWNLOADER_H__ */
