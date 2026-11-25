"""
Element metadata from the BPMN files the bridge deploys: per executable process,
element id -> name, BPMN tag (userTask, sendTask, ...) and lane name.

Used by the Camunda log (core.Bridge.camunda_log_once): XES activity names come from the
model itself (exactly what conformance checking compares against) and the lane becomes
the XES org:resource. Elements inside a subprocess inherit the subprocess's lane.
Stdlib only.
"""
import glob
import os
import xml.etree.ElementTree as ET

B = "{http://www.omg.org/spec/BPMN/20100524/MODEL}"


def load(bpmn_dir):
    out = {}
    for path in sorted(glob.glob(os.path.join(bpmn_dir, "*.bpmn"))):
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            continue                       # the deployment will report it; don't crash here
        for proc in root.iter(B + "process"):
            if proc.get("isExecutable") != "true":
                continue
            out[proc.get("id")] = _process(proc)
    return out


def _process(proc):
    lane_of = {}
    for lane in proc.iter(B + "lane"):
        for ref in lane.findall(B + "flowNodeRef"):
            lane_of[(ref.text or "").strip()] = lane.get("name") or lane.get("id")
    meta = {}

    def walk(container, inherited_lane):
        for el in container:
            if el.tag in (B + "laneSet", B + "extensionElements"):
                continue
            eid = el.get("id")
            if not eid:
                continue
            lane = lane_of.get(eid, inherited_lane)
            meta[eid] = {"name": el.get("name") or "", "tag": el.tag.replace(B, ""), "lane": lane}
            if el.tag == B + "subProcess":
                walk(el, lane)

    walk(proc, None)
    return meta
