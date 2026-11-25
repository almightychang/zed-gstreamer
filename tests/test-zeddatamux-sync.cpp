#define PACKAGE "regression-test"
#define GST_PACKAGE_VERSION "test"
#define GST_PACKAGE_LICENSE "LGPL"
#define GST_PACKAGE_NAME "regression-test"
#define GST_PACKAGE_ORIGIN "local"
#include "gst-zed-data-mux/gstzeddatamux.cpp"

// Frame k has PTS k, data metadata image_timestamp_ns = k, and video bytes = k % 256.
struct Output {
    int received = 0;
    int wrong_metadata = 0;
};

static GstFlowReturn consume(GstPad *pad, GstObject *, GstBuffer *buf) {
    auto *out = (Output *) g_object_get_data(G_OBJECT(pad), "output");
    auto *meta = (GstZedSrcMeta *) gst_buffer_get_meta(buf, GST_ZED_SRC_META_API_TYPE);
    if (!meta || meta->image_timestamp_ns != GST_BUFFER_PTS(buf)) ++out->wrong_metadata;
    ++out->received;
    gst_buffer_unref(buf);
    return GST_FLOW_OK;
}

static GstBuffer *data_frame(guint64 k) {
    GstBuffer *buf = gst_buffer_new_allocate(NULL, sizeof(GstZedSrcMeta), NULL);
    GstMapInfo map;
    g_assert_true(gst_buffer_map(buf, &map, GST_MAP_WRITE));
    memset(map.data, 0, map.size);
    ((GstZedSrcMeta *) map.data)->image_timestamp_ns = k;
    gst_buffer_unmap(buf, &map);
    GST_BUFFER_PTS(buf) = k;
    return buf;
}

static GstBuffer *video_frame(guint64 k, gsize size) {
    GstBuffer *buf = gst_buffer_new_allocate(NULL, size, NULL);
    gst_buffer_memset(buf, 0, k % 256, size);
    GST_BUFFER_PTS(buf) = k;
    return buf;
}

struct Harness {
    GstZedDataMux *mux;
    GstPad *sink;
    Output out;

    Harness() {
        mux = GST_ZEDDATAMUX(g_object_new(gst_zeddatamux_get_type(), NULL));
        sink = gst_pad_new("consumer", GST_PAD_SINK);
        g_object_set_data(G_OBJECT(sink), "output", &out);
        gst_pad_set_chain_function(sink, consume);
        gst_pad_set_active(sink, TRUE);
        g_assert_cmpint(gst_pad_link(mux->srcpad, sink), ==, GST_PAD_LINK_OK);
        play();
    }

    ~Harness() {
        g_assert_cmpint(out.wrong_metadata, ==, 0);
        gst_element_set_state(GST_ELEMENT(mux), GST_STATE_NULL);
        gst_object_unref(mux);
        gst_object_unref(sink);
    }

    void play() {
        g_assert_cmpint(gst_element_set_state(GST_ELEMENT(mux), GST_STATE_PLAYING), ==,
                        GST_STATE_CHANGE_SUCCESS);
        gst_pad_push_event(mux->srcpad, gst_event_new_stream_start("test"));
        GstSegment segment;
        gst_segment_init(&segment, GST_FORMAT_TIME);
        gst_pad_push_event(mux->srcpad, gst_event_new_segment(&segment));
    }

    GstFlowReturn data(guint64 k) {
        return gst_zeddatamux_chain_data(mux->sinkpad_data, GST_OBJECT(mux), data_frame(k));
    }

    GstFlowReturn video(guint64 k, gsize size = 16) {
        return gst_zeddatamux_chain_video(mux->sinkpad_video, GST_OBJECT(mux), video_frame(k, size));
    }

    void data_eos() { gst_pad_send_event(mux->sinkpad_data, gst_event_new_eos()); }
};

// Documented pipeline: data comes straight from zedsrc, video goes through a queue first,
// so the data branch is usually one frame ahead.
static void test_video_one_frame_behind_is_muxed() {
    Harness h;
    h.data(1);
    h.data(2);
    h.video(1);
    h.video(2);
    g_assert_cmpint(h.out.received, ==, 2);
}

// The data branch ends first. Video frame 3 is still on its way through the queue.
// Proposed behavior: the GStreamer design docs allow, but do not require, waiting for EOS on
// all sink pads.
static void test_eos_on_data_waits_for_video() {
    Harness h;
    for (guint64 k = 1; k <= 2; ++k) {
        h.data(k);
        h.video(k);
    }
    h.data(3);
    h.data_eos();
    GstFlowReturn ret = h.video(3);
    g_assert_cmpint(h.out.received, ==, 3);
    g_assert_cmpstr(gst_flow_get_name(ret), ==, gst_flow_get_name(GST_FLOW_OK));
}

// Stop and play again: timestamps restart from the beginning. In the first run each pad was
// the first to arrive at least once (100: video first, 101: data first).
static void test_restart_resets_matching() {
    Harness h;
    h.video(100);
    h.data(100);
    h.data(101);
    h.video(101);
    g_assert_cmpint(h.out.received, ==, 2);
    gst_element_set_state(GST_ELEMENT(h.mux), GST_STATE_NULL);
    h.play();
    for (guint64 k = 1; k <= 10; ++k) {
        h.data(k);
        h.video(k);
    }
    g_assert_cmpint(h.out.received, ==, 12);
}

int main(int argc, char **argv) {
    gst_init(&argc, &argv);
    g_test_init(&argc, &argv, NULL);
    g_test_set_nonfatal_assertions();
    g_test_add_func("/zeddatamux/video-one-frame-behind-is-muxed",
                    test_video_one_frame_behind_is_muxed);
    g_test_add_func("/zeddatamux/eos-on-data-waits-for-video", test_eos_on_data_waits_for_video);
    g_test_add_func("/zeddatamux/restart-resets-matching", test_restart_resets_matching);
    return g_test_run();
}
