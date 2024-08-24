// benjineering 👍
#ifndef __GST_S3_SRC_H__
#define __GST_S3_SRC_H__

#include <gst/gst.h>
#include <gst/base/gstbasesrc.h>

#include "gsts3downloader.h"
#include "gstawscredentials.h"

G_BEGIN_DECLS

#define GST_TYPE_S3_SRC \
  (gst_s3_src_get_type())
#define GST_S3_SRC(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj), GST_TYPE_S3_SRC, GstS3Src))
#define GST_S3_SRC_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_CAST((klass), GST_TYPE_S3_SRC, GstS3SrcClass))
#define GST_IS_S3_SRC(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GST_TYPE_S3_SRC))
#define GST_IS_S3_SRC_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GST_TYPE_S3_SRC))
#define GST_S3_SRC_CAST(obj) ((GstS3Src *)(obj))
typedef struct _GstS3Src GstS3Src;
typedef struct _GstS3SrcClass GstS3SrcClass;

/**
 * GstS3Src:
 *
 * Opaque #GstS3Src structure.
 */
struct _GstS3Src
{
  GstBaseSrc parent;

  /*< private > */
  GstS3Config config;

  GstS3Downloader *downloader;

  gchar *buffer;
  gsize current_buffer_size;
  gsize total_bytes_read;

  gboolean is_started;
};

struct _GstS3SrcClass
{
  GstBaseSrcClass parent_class;
};

GST_EXPORT
GType gst_s3_src_get_type(void);

G_END_DECLS

#endif /* __GST_S3_SRC_H__ */
