// benjineering 👍

/**
 * SECTION:element-s3src
 * @title: s3src
 *
 * Read data from a Amazon S3 bucket file.
 *
 * ## Example launch line
 * |[
 * gst-launch-1.0 s3src bucket=test-bucket key=in.file ! etc ! s3src bucket=test-bucket key=out.file
 * ]|
 *
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <string.h>

#include <gst/gst.h>
#include <gst/gsturi.h>

#include "gsts3src.h"
#include "gsts3multipartdownloader.h"

static GstStaticPadTemplate srctemplate = GST_STATIC_PAD_TEMPLATE("src",
                                                                  GST_PAD_SRC,
                                                                  GST_PAD_ALWAYS,
                                                                  GST_STATIC_CAPS_ANY);

GST_DEBUG_CATEGORY(gst_s3_src_debug);
#define GST_CAT_DEFAULT gst_s3_src_debug

#define MIN_BUFFER_SIZE 5 * 1024 * 1024
#define DEFAULT_BUFFER_SIZE GST_S3_CONFIG_DEFAULT_BUFFER_SIZE
#define DEFAULT_BUFFER_COUNT GST_S3_CONFIG_DEFAULT_BUFFER_COUNT

#define REQUIRED_BUT_UNUSED(x) (void)(x)

enum
{
  PROP_0,
  PROP_BUCKET,
  PROP_KEY,
  PROP_LOCATION,
  PROP_ACL,
  PROP_CONTENT_TYPE,
  PROP_CA_FILE,
  PROP_REGION,
  PROP_BUFFER_SIZE,
  PROP_INIT_AWS_SDK,
  PROP_CREDENTIALS,
  PROP_AWS_SDK_ENDPOINT,
  PROP_AWS_SDK_USE_HTTP,
  PROP_AWS_SDK_VERIFY_SSL,
  PROP_AWS_SDK_S3_SIGN_PAYLOAD,
  PROP_LAST
};

static void gst_s3_src_dispose(GObject *object);

static void gst_s3_src_set_property(GObject *object, guint prop_id,
                                    const GValue *value, GParamSpec *pspec);
static void gst_s3_src_get_property(GObject *object, guint prop_id,
                                    GValue *value, GParamSpec *pspec);

static gboolean gst_s3_src_start(GstBaseSrc *src);
static gboolean gst_s3_src_stop(GstBaseSrc *src);
static gboolean gst_s3_src_event(GstBaseSrc *src, GstEvent *event);
static GstFlowReturn gst_s3_src_render(GstBaseSrc *src,
                                       GstBuffer *buffer);
static gboolean gst_s3_src_query(GstBaseSrc *bsrc, GstQuery *query);

static gboolean gst_s3_src_fill_buffer(GstS3Src *src, GstBuffer *buffer);
static gboolean gst_s3_src_flush_buffer(GstS3Src *src);

/**
 * GstURIHandler Interface implementation
 */
static GstURIType
gst_s3_src_urihandler_get_type(GType type)
{
  REQUIRED_BUT_UNUSED(type);
  return GST_URI_SRC;
}

static const gchar *const *
gst_s3_src_urihandler_get_protocols(GType type)
{
  REQUIRED_BUT_UNUSED(type);
  static const gchar *protocols[] = {"s3", NULL};
  return protocols;
}

static gchar *
gst_s3_src_urihandler_get_uri(GstURIHandler *handler)
{
  GValue value = {0};
  g_object_get_property(G_OBJECT(handler), "location", &value);
  return g_strdup_value_contents(&value);
}

static gboolean
gst_s3_src_urihandler_set_uri(GstURIHandler *handler, const gchar *uri, GError **error)
{
  REQUIRED_BUT_UNUSED(error);
  g_object_set(G_OBJECT(handler), "location", uri, NULL);
  return TRUE;
}

static void
gst_s3_src_urihandler_init(gpointer g_iface, gpointer iface_data)
{
  REQUIRED_BUT_UNUSED(iface_data);
  GstURIHandlerInterface *iface = (GstURIHandlerInterface *)g_iface;
  iface->get_type = gst_s3_src_urihandler_get_type;
  iface->get_protocols = gst_s3_src_urihandler_get_protocols;
  iface->get_uri = gst_s3_src_urihandler_get_uri;
  iface->set_uri = gst_s3_src_urihandler_set_uri;
}

#define gst_s3_src_parent_class parent_class
G_DEFINE_TYPE_WITH_CODE(GstS3Src, gst_s3_src, GST_TYPE_BASE_SRC,
                        G_IMPLEMENT_INTERFACE(GST_TYPE_URI_HANDLER, gst_s3_src_urihandler_init));

static void
gst_s3_src_class_init(GstS3SrcClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS(klass);
  GstElementClass *gstelement_class = GST_ELEMENT_CLASS(klass);
  GstBaseSrcClass *gstbasesrc_class = GST_BASE_SRC_CLASS(klass);

  GST_DEBUG_CATEGORY_INIT(gst_s3_src_debug, "s3src", 0, "s3src element");

  gobject_class->dispose = gst_s3_src_dispose;
  gobject_class->set_property = gst_s3_src_set_property;
  gobject_class->get_property = gst_s3_src_get_property;

  g_object_class_install_property(gobject_class, PROP_BUCKET,
                                  g_param_spec_string("bucket", "S3 bucket",
                                                      "The bucket of the file to write (ignored when 'location' is set)", NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_KEY,
                                  g_param_spec_string("key", "S3 key",
                                                      "The key of the file to write (ignored when 'location' is set)", NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_LOCATION,
                                  g_param_spec_string("location", "S3 URI",
                                                      "The URI of the file to write", NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_ACL,
                                  g_param_spec_string("acl", "S3 object acl",
                                                      "The canned acl for s3 object to download", NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_CONTENT_TYPE,
                                  g_param_spec_string("content-type", "Content type",
                                                      "The content type of a stream", NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_CA_FILE,
                                  g_param_spec_string("ca-file", "CA file",
                                                      "A path to a CA file", NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_REGION,
                                  g_param_spec_string("region", "AWS Region",
                                                      "An AWS region (e.g. eu-west-2). Leave empty for region-autodetection "
                                                      "(Please note region-autodetection requires an extra network call)",
                                                      NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_BUFFER_SIZE,
                                  g_param_spec_uint("buffer-size", "Buffering size",
                                                    "Size of buffer in number of bytes", MIN_BUFFER_SIZE,
                                                    G_MAXUINT, DEFAULT_BUFFER_SIZE,
                                                    G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_INIT_AWS_SDK,
                                  g_param_spec_boolean("init-aws-sdk", "Init AWS SDK",
                                                       "Whether to initialize AWS SDK",
                                                       GST_S3_CONFIG_DEFAULT_INIT_AWS_SDK,
                                                       G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_CREDENTIALS,
                                  g_param_spec_boxed("aws-credentials", "AWS credentials",
                                                     "The AWS credentials to use", GST_TYPE_AWS_CREDENTIALS,
                                                     G_PARAM_WRITABLE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_AWS_SDK_ENDPOINT,
                                  g_param_spec_string("aws-sdk-endpoint", "AWS SDK Endpoint",
                                                      "AWS SDK endpoint override (ip:port)", NULL,
                                                      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_AWS_SDK_USE_HTTP,
                                  g_param_spec_boolean("aws-sdk-use-http", "AWS SDK Use HTTP",
                                                       "Whether to enable http for the AWS SDK (default https)",
                                                       GST_S3_CONFIG_DEFAULT_PROP_AWS_SDK_USE_HTTP,
                                                       G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_AWS_SDK_VERIFY_SSL,
                                  g_param_spec_boolean("aws-sdk-verify-ssl", "AWS SDK Verify SSL",
                                                       "Whether to enable/disable tls validation for the AWS SDK",
                                                       GST_S3_CONFIG_DEFAULT_PROP_AWS_SDK_VERIFY_SSL,
                                                       G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property(gobject_class, PROP_AWS_SDK_S3_SIGN_PAYLOAD,
                                  g_param_spec_boolean("aws-sdk-s3-sign-payload", "AWS SDK S3 Sign Payload",
                                                       "Whether to have the AWS SDK S3 client sign payloads using the Auth v4 Signer",
                                                       GST_S3_CONFIG_DEFAULT_PROP_AWS_SDK_S3_SIGN_PAYLOAD,
                                                       G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  gst_element_class_set_static_metadata(gstelement_class,
                                        "S3 Src",
                                        "Src/S3", "Write stream to an Amazon S3 bucket",
                                        "Marcin Kolny <marcin.kolny at gmail.com>");
  gst_element_class_add_static_pad_template(gstelement_class, &srctemplate);

  gstbasesrc_class->start = GST_DEBUG_FUNCPTR(gst_s3_src_start);
  gstbasesrc_class->stop = GST_DEBUG_FUNCPTR(gst_s3_src_stop);
  gstbasesrc_class->query = GST_DEBUG_FUNCPTR(gst_s3_src_query);
  gstbasesrc_class->render = GST_DEBUG_FUNCPTR(gst_s3_src_render);
  gstbasesrc_class->event = GST_DEBUG_FUNCPTR(gst_s3_src_event);
}

static void
gst_s3_destroy_downloader(GstS3Src *src)
{
  if (src->downloader)
  {
    gst_s3_downloader_destroy(src->downloader);
    src->downloader = NULL;
  }
}

static void
gst_s3_src_init(GstS3Src *s3src)
{
  s3src->config = GST_S3_CONFIG_INIT;
  s3src->config.credentials = gst_aws_credentials_new_default();
  s3src->downloader = NULL;
  s3src->is_started = FALSE;

  gst_base_src_set_sync(GST_BASE_SINK(s3src), FALSE);
}

static void
gst_s3_src_release_config(GstS3Config *config)
{
  g_free(config->region);
  g_free(config->bucket);
  g_free(config->key);
  g_free(config->location);
  g_free(config->acl);
  g_free(config->content_type);
  g_free(config->ca_file);
  g_free(config->aws_sdk_endpoint);
  gst_aws_credentials_free(config->credentials);

  *config = GST_S3_CONFIG_INIT;
}

static void
gst_s3_src_dispose(GObject *object)
{
  GstS3Src *src = GST_S3_SINK(object);

  gst_s3_src_release_config(&src->config);

  gst_s3_destroy_downloader(src);

  G_OBJECT_CLASS(parent_class)->dispose(object);
}

static void
gst_s3_src_set_string_property(GstS3Src *src, const gchar *value,
                               gchar **property, const gchar *property_name)
{
  if (src->is_started)
  {
    GST_WARNING("Changing the `%s' property on s3src "
                "when streaming has started is not supported.",
                property_name);
    return;
  }

  g_free(*property);

  if (value != NULL)
  {
    *property = g_strdup(value);
    GST_INFO_OBJECT(src, "%s : %s", property_name, *property);
  }
  else
  {
    *property = NULL;
  }
}

static void
gst_s3_src_set_property(GObject *object, guint prop_id,
                        const GValue *value, GParamSpec *pspec)
{
  GstS3Src *src = GST_S3_SINK(object);

  switch (prop_id)
  {
  case PROP_BUCKET:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.bucket, "bucket");
    break;
  case PROP_KEY:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.key, "key");
    break;
  case PROP_LOCATION:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.location, "location");
    break;
  case PROP_ACL:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.acl, "acl");
    break;
  case PROP_CONTENT_TYPE:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.content_type, "content-type");
    break;
  case PROP_CA_FILE:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.ca_file, "ca-file");
    break;
  case PROP_REGION:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.region, "region");
    break;
  case PROP_BUFFER_SIZE:
    if (src->is_started)
    {
      // TODO: this could be supported in the future
      GST_WARNING("Changing buffer-size property after starting the element is not supported yet.");
    }
    else
    {
      src->config.buffer_size = g_value_get_uint(value);
    }
    break;
  case PROP_INIT_AWS_SDK:
    src->config.init_aws_sdk = g_value_get_boolean(value);
    break;
  case PROP_CREDENTIALS:
    if (src->config.credentials)
      gst_aws_credentials_free(src->config.credentials);
    src->config.credentials = gst_aws_credentials_copy(g_value_get_boxed(value));
    break;
  case PROP_AWS_SDK_ENDPOINT:
    gst_s3_src_set_string_property(src, g_value_get_string(value),
                                   &src->config.aws_sdk_endpoint, "aws-sdk-endpoint");
    break;
  case PROP_AWS_SDK_USE_HTTP:
    src->config.aws_sdk_use_http = g_value_get_boolean(value);
    break;
  case PROP_AWS_SDK_VERIFY_SSL:
    src->config.aws_sdk_verify_ssl = g_value_get_boolean(value);
    break;
  case PROP_AWS_SDK_S3_SIGN_PAYLOAD:
    src->config.aws_sdk_s3_sign_payload = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gst_s3_src_get_property(GObject *object, guint prop_id, GValue *value,
                        GParamSpec *pspec)
{
  GstS3Src *src = GST_S3_SINK(object);

  switch (prop_id)
  {
  case PROP_BUCKET:
    g_value_set_string(value, src->config.bucket);
    break;
  case PROP_KEY:
    g_value_set_string(value, src->config.key);
    break;
  case PROP_LOCATION:
    g_value_set_string(value, src->config.location);
    break;
  case PROP_ACL:
    g_value_set_string(value, src->config.acl);
    break;
  case PROP_CONTENT_TYPE:
    g_value_set_string(value, src->config.content_type);
    break;
  case PROP_CA_FILE:
    g_value_set_string(value, src->config.ca_file);
    break;
  case PROP_REGION:
    g_value_set_string(value, src->config.region);
    break;
  case PROP_BUFFER_SIZE:
    g_value_set_uint(value, src->config.buffer_size);
    break;
  case PROP_INIT_AWS_SDK:
    g_value_set_boolean(value, src->config.init_aws_sdk);
    break;
  case PROP_AWS_SDK_ENDPOINT:
    g_value_set_string(value, src->config.aws_sdk_endpoint);
    break;
  case PROP_AWS_SDK_USE_HTTP:
    g_value_set_boolean(value, src->config.aws_sdk_use_http);
    break;
  case PROP_AWS_SDK_VERIFY_SSL:
    g_value_set_boolean(value, src->config.aws_sdk_verify_ssl);
    break;
  case PROP_AWS_SDK_S3_SIGN_PAYLOAD:
    g_value_set_boolean(value, src->config.aws_sdk_s3_sign_payload);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static gboolean
gst_s3_src_is_null_or_empty(const gchar *str)
{
  return str == NULL || str[0] == '\0';
}

static gboolean
gst_s3_src_start(GstBaseSrc *basesrc)
{
  GstS3Src *src = GST_S3_SINK(basesrc);

  if (gst_s3_src_is_null_or_empty(src->config.location) && (gst_s3_src_is_null_or_empty(src->config.bucket) || gst_s3_src_is_null_or_empty(src->config.key)))
    goto no_destination;

  if (src->downloader == NULL)
  {
    src->downloader = gst_s3_multipart_downloader_new(&src->config);
  }

  if (!src->downloader)
    goto init_failed;

  g_free(src->buffer);
  src->buffer = NULL;

  src->buffer = g_malloc(src->config.buffer_size);
  src->current_buffer_size = 0;
  src->total_bytes_written = 0;

  if (gst_s3_src_is_null_or_empty(src->config.location))
  {
    GST_DEBUG_OBJECT(src, "started S3 download %s %s",
                     src->config.bucket, src->config.key);
  }
  else
  {
    GST_DEBUG_OBJECT(src, "started S3 download %s", src->config.location);
  }

  src->is_started = TRUE;

  return TRUE;

  /* ERRORS */
no_destination:
{
  GST_ELEMENT_ERROR(src, RESOURCE, NOT_FOUND,
                    ("No bucket or key specified for writing."), (NULL));
  return FALSE;
}

init_failed:
{
  gst_s3_destroy_downloader(src);
  GST_ELEMENT_ERROR(src, RESOURCE, OPEN_WRITE,
                    ("Unable to initialize S3 downloader."), (NULL));
  return FALSE;
}
}

static gboolean
gst_s3_src_stop(GstBaseSrc *basesrc)
{
  GstS3Src *src = GST_S3_SINK(basesrc);
  gboolean ret = TRUE;

  if (src->buffer)
  {
    gst_s3_src_flush_buffer(src);
    ret = gst_s3_downloader_complete(src->downloader);

    g_free(src->buffer);
    src->buffer = NULL;
    src->current_buffer_size = 0;
    src->total_bytes_written = 0;
  }

  gst_s3_destroy_downloader(src);

  src->is_started = FALSE;

  return ret;
}

static gboolean
gst_s3_src_query(GstBaseSrc *base_src, GstQuery *query)
{
  gboolean ret = FALSE;
  GstS3Src *src;

  src = GST_S3_SINK(base_src);

  switch (GST_QUERY_TYPE(query))
  {
  case GST_QUERY_FORMATS:
    gst_query_set_formats(query, 2, GST_FORMAT_DEFAULT, GST_FORMAT_BYTES);
    ret = TRUE;
    break;

  case GST_QUERY_POSITION:
  {
    GstFormat format;

    gst_query_parse_position(query, &format, NULL);

    switch (format)
    {
    case GST_FORMAT_DEFAULT:
    case GST_FORMAT_BYTES:
      gst_query_set_position(query, GST_FORMAT_BYTES,
                             src->total_bytes_written);
      ret = TRUE;
      break;
    default:
      break;
    }
    break;
  }

  case GST_QUERY_SEEKING:
  {
    GstFormat fmt;

    gst_query_parse_seeking(query, &fmt, NULL, NULL, NULL);
    gst_query_set_seeking(query, fmt, FALSE, 0, -1);
    ret = TRUE;
    break;
  }
  default:
    ret = GST_BASE_SINK_CLASS(parent_class)->query(base_src, query);
    break;
  }
  return ret;
}

static gboolean
gst_s3_src_event(GstBaseSrc *base_src, GstEvent *event)
{
  GstEventType type;
  GstS3Src *src;

  src = GST_S3_SINK(base_src);
  type = GST_EVENT_TYPE(event);

  switch (type)
  {
  case GST_EVENT_EOS:
    gst_s3_src_flush_buffer(src);
    break;
  default:
    break;
  }

  return GST_BASE_SINK_CLASS(parent_class)->event(base_src, event);
}

static GstFlowReturn
gst_s3_src_render(GstBaseSrc *base_src, GstBuffer *buffer)
{
  GstS3Src *src;
  GstFlowReturn flow;
  guint8 n_mem;

  src = GST_S3_SINK(base_src);

  n_mem = gst_buffer_n_memory(buffer);

  if (n_mem > 0)
  {
    if (gst_s3_src_fill_buffer(src, buffer))
    {
      flow = GST_FLOW_OK;
    }
    else
    {
      GST_WARNING("Failed to flush the internal buffer");
      flow = GST_FLOW_ERROR;
    }
  }
  else
  {
    flow = GST_FLOW_OK;
  }

  return flow;
}

static gboolean
gst_s3_src_flush_buffer(GstS3Src *src)
{
  gboolean ret = TRUE;

  if (src->current_buffer_size)
  {
    ret = gst_s3_downloader_download_part(src->downloader, src->buffer,
                                          src->current_buffer_size);
    src->current_buffer_size = 0;
  }

  return ret;
}

static gboolean
gst_s3_src_fill_buffer(GstS3Src *src, GstBuffer *buffer)
{
  GstMapInfo map_info = GST_MAP_INFO_INIT;
  gsize ptr = 0;
  gsize bytes_to_copy;

  if (!gst_buffer_map(buffer, &map_info, GST_MAP_READ))
    goto map_failed;

  do
  {
    bytes_to_copy =
        MIN(src->config.buffer_size - src->current_buffer_size,
            map_info.size - ptr);
    memcpy(src->buffer + src->current_buffer_size, map_info.data + ptr,
           bytes_to_copy);
    src->current_buffer_size += bytes_to_copy;
    if (src->current_buffer_size == src->config.buffer_size)
    {
      if (!gst_s3_src_flush_buffer(src))
      {
        return FALSE;
      }
    }
    ptr += bytes_to_copy;
    src->total_bytes_written += bytes_to_copy;
  } while (ptr < map_info.size);

  gst_buffer_unmap(buffer, &map_info);
  return TRUE;

map_failed:
{
  GST_ELEMENT_ERROR(src, RESOURCE, NOT_FOUND,
                    ("Failed to map the buffer."), (NULL));
  return FALSE;
}
}
