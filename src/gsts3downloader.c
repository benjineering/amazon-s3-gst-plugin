// benjineering 👍
#include "gsts3downloader.h"

#define GET_CLASS_(downloader) ((GstS3Downloader *)(downloader))->klass

void gst_s3_uploader_destroy(GstS3Downloader *downloader)
{
  GET_CLASS_(downloader)->destroy(downloader);
}

gboolean
gst_s3_uploader_upload_part(GstS3Downloader *downloader,
                            const gchar *buffer, gsize size)
{
  return GET_CLASS_(downloader)->upload_part(downloader, buffer, size);
}

gboolean
gst_s3_uploader_complete(GstS3Downloader *downloader)
{
  return GET_CLASS_(downloader)->complete(downloader);
}
