#define PACKAGE "regression-test"
#define GST_PACKAGE_VERSION "test"
#define GST_PACKAGE_LICENSE "LGPL"
#define GST_PACKAGE_NAME "regression-test"
#define GST_PACKAGE_ORIGIN "local"
#include "gst-zed-demux/gstzeddemux.cpp"

struct Branch {
    GstPad *sink;
    int received;
    GstFlowReturn result;
};

static GstFlowReturn consume(GstPad *pad, GstObject *, GstBuffer *buf) {
    auto *branch = (Branch *) g_object_get_data(G_OBJECT(pad), "branch");
    ++branch->received;
    gst_buffer_unref(buf);
    return branch->result;
}

static void attach(GstPad *src, Branch *branch) {
    branch->sink = gst_pad_new(NULL, GST_PAD_SINK);
    g_object_set_data(G_OBJECT(branch->sink), "branch", branch);
    gst_pad_set_chain_function(branch->sink, consume);
    gst_pad_set_active(branch->sink, TRUE);
    g_assert_cmpint(gst_pad_link(src, branch->sink), ==, GST_PAD_LINK_OK);
}

static void start_stream(GstPad *src) {
    gst_pad_set_active(src, TRUE);
    gst_pad_push_event(src, gst_event_new_stream_start("test"));
    GstSegment segment;
    gst_segment_init(&segment, GST_FORMAT_TIME);
    gst_pad_push_event(src, gst_event_new_segment(&segment));
}

static GstFlowReturn push_side_by_side(GstZedDemux *demux) {
    GstBuffer *buf = gst_buffer_new_allocate(NULL, 64, NULL);
    GST_BUFFER_PTS(buf) = 0;
    return gst_zeddemux_chain(demux->sinkpad, GST_OBJECT(demux), buf);
}

// The right image goes out on src_aux, which is linked. src_left is not.
static void test_unlinked_left_still_feeds_aux() {
    auto *demux = GST_ZEDDEMUX(g_object_new(gst_zeddemux_get_type(), NULL));
    Branch aux = {NULL, 0, GST_FLOW_OK};
    attach(demux->srcpad_aux, &aux);
    start_stream(demux->srcpad_left);
    start_stream(demux->srcpad_aux);

    GstFlowReturn ret = push_side_by_side(demux);

    g_assert_cmpint(aux.received, ==, 1);
    g_assert_cmpstr(gst_flow_get_name(ret), ==, gst_flow_get_name(GST_FLOW_OK));
    gst_object_unref(demux);
    gst_object_unref(aux.sink);
}

// One branch has finished (e.g. a sink with num-buffers). The other keeps going.
static void test_eos_on_left_still_feeds_aux() {
    auto *demux = GST_ZEDDEMUX(g_object_new(gst_zeddemux_get_type(), NULL));
    Branch left = {NULL, 0, GST_FLOW_EOS};
    Branch aux = {NULL, 0, GST_FLOW_OK};
    attach(demux->srcpad_left, &left);
    attach(demux->srcpad_aux, &aux);
    start_stream(demux->srcpad_left);
    start_stream(demux->srcpad_aux);

    GstFlowReturn ret = push_side_by_side(demux);

    g_assert_cmpint(left.received, ==, 1);
    g_assert_cmpint(aux.received, ==, 1);
    g_assert_cmpstr(gst_flow_get_name(ret), ==, gst_flow_get_name(GST_FLOW_OK));
    gst_object_unref(demux);
    gst_object_unref(left.sink);
    gst_object_unref(aux.sink);
}

int main(int argc, char **argv) {
    gst_init(&argc, &argv);
    g_test_init(&argc, &argv, NULL);
    g_test_set_nonfatal_assertions();
    g_test_add_func("/zeddemux/unlinked-left-still-feeds-aux", test_unlinked_left_still_feeds_aux);
    g_test_add_func("/zeddemux/eos-on-left-still-feeds-aux", test_eos_on_left_still_feeds_aux);
    return g_test_run();
}
