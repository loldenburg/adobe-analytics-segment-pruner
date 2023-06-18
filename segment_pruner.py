# This is a slightly shortened and modified version of the Segment Pruner, part of the Adobe Analytics Component Manager for Google Sheets.
# A step-by-step tutorial for this code is at https://thebounce.io/making-large-segments-small-again-b4d4aa089200
#
# If you prefer pruning segments comfortably from a Google Sheet, check out the Component Manager here:
# https://docs.datacroft.de/main-functions/segment-pruner
import copy
import datetime as dt
from json import dumps
import aanalytics2 as aa2

ags = aa2.Login() # READ NEXT ROWS!
# ---
# ATTENTION: We assume you have imported the aanalytics2
# module, logged in to the client and referenced the module as `ags`.
# Everybody does this differently, so please refer to https://github.com/pitchmuc/adobe-analytics-api-2.0/blob/master/docs/getting_started.md
# for the optimum solution in your case 
# ---

rs_id = "the_report_suite_id"
seg_id = "s3537_646796b50f59414c34dcacbf"
metric_ids = ["metrics/occurrences", "metrics/orders"]
days_back = 90
# 'func' values that indicate a group of elements e.g. "container") and not an actual filter (e.g. "page == home")
grouping_functions = ["segment", "container", "and", "or", "without", "sequence", "sequence-prefix",
                      "sequence-suffix", "sequence-and", "sequence-or"]

# finds and returns a subdictionary with a certain key inside of a multi-nested dictionary, e.g. "_id = 7"
def find_subdictionary_by_id(d: dict = None, target_id: int = None, key: str = "_id"):
    if key in d and d[key] == target_id:
        return d

    for value in d.values():
        if isinstance(value, dict):
            result = find_subdictionary_by_id(value, target_id)
            if result is not None:
                return result

    return None

# deletes a subdictionary with a certain key, e.g. "_id = 7"
def delete_subdict_by_id(d: dict = None, subdict_id: int = None, key: str = "_id"):
    d = set_subdict_to_none(d=d, subdict_id=subdict_id, key=key)
    return remove_nones_from_dict(d=d)


# sets a subdictionary with a certain key to None, e.g. "_id = 7"
def set_subdict_to_none(d: dict = None, subdict_id: int = None, key: str = "_id"):
    # Find the sub-dictionary to delete
    if isinstance(d, dict):
        if key in d and d[key] == subdict_id:
            return None
        return {k: set_subdict_to_none(v, subdict_id) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [set_subdict_to_none(v, subdict_id) for v in d if v is not None]
    else:
        return d


# replaces a subdictionary with a certain key, e.g. "_id = 7" by a new one
def replace_subdict_by_id(d: dict = None, subdict_id: dict = None, key: str = "_id", replace_by: dict = None):
    # Find the sub-dictionary to delete
    if isinstance(d, dict):
        if key in d and d[key] == subdict_id:
            return replace_by
        return {k: replace_subdict_by_id(v, subdict_id, "_id", replace_by) for k, v in d.items() if
                v is not None}
    elif isinstance(d, list):
        return [replace_subdict_by_id(v, subdict_id, "_id", replace_by) for v in d if v is not None]
    else:
        return d


# deletes all keys with name _key from a dictionary
def delete_keys_from_dict(d: dict = None, _key: str = "_id"):
    if isinstance(d, dict):
        for key in list(d.keys()):  # Create a list of keys to iterate over
            if key == _key:
                del d[key]
            else:
                delete_keys_from_dict(d[key])
    elif isinstance(d, list):
        for item in d:
            delete_keys_from_dict(item)


# removes all None values from a dictionary
def remove_nones_from_dict(d: dict = None):
    if isinstance(d, dict):
        return {k: remove_nones_from_dict(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [remove_nones_from_dict(v) for v in d if v is not None]
    else:
        return d

# returns True if a dict_to_find is a sub-dictionary of a dict_to_search (works only within segment grouping functions)
def is_part_of_larger_dict(dict_to_find, dict_to_search):
    if dict_to_search.get("preds") is not None:
        for el in dict_to_search["preds"]:
            if el == dict_to_find:
                return True
    return False


# returns True if the key is found in the dictionary, False otherwise
def key_exists_in_dict(key: str = None, dct: dict = None):
    for k, v in dct.items():
        if k == key:
            return True
        elif isinstance(v, dict):
            return key_exists_in_dict(key, v)
        elif isinstance(v, list):
            for el in v:
                if isinstance(el, dict):
                    if key_exists_in_dict(key, el) is True:
                        return True  # in the case of False we need to continue with the next element!
    return False


# returns True if at least one key-value pair is found where the value does NOT equal any of the values in the whitelist, False otherwise
def at_least_once_in_dict(key: str = None, values_whitelist: list = None, dct: dict = None):
    for k, v in dct.items():
        if (k == key) and (v not in values_whitelist):
            print(f"Found a key '{k}' whose value '{v}' is not in the list of values.")
            return True
        elif isinstance(v, dict):
            return at_least_once_in_dict(key, values_whitelist, v)
        elif (isinstance(v, list)):
            for el in v:
                if isinstance(el, dict):
                    if at_least_once_in_dict(key, values_whitelist, el) is True:
                        return True  # in the case of True (element was found), we don't need to check the next element
    return False  # if we get here, no key-value pair of the desired key-value combination was found


# assigns incrementing IDs with key "_id" to all dictionaries found. Also traverses lists for that
def assign_ids_recursive(data, id_counter=None):
    if id_counter is None:
        id_counter = {'_id': 0}

    if isinstance(data, dict):
        data['_id'] = id_counter['_id']  # Assign ID to current dictionary
        id_counter['_id'] += 1

        for value in data.values():
            assign_ids_recursive(value, id_counter)  # Recursively process sub-dictionaries

    elif isinstance(data, list):
        for item in data:
            assign_ids_recursive(item, id_counter)  # Recursively process list items


# Finds empty preds [] and writes the _ids of their parent dictionary into a list so we can delete them via delete_subdict_by_id
def find_empty_arrays(d: dict = None, the_id: int = None, ids_to_del: list = None):
    for k, v in d.items():
        if k == "pred":  # container
            find_empty_arrays(v, the_id=v["_id"], ids_to_del=ids_to_del)
        if k == "preds":  # list of elements
            if len(v) == 0:
                ids_to_del.append(the_id)
            else:
                for el in v:
                    if el.get("pred") is not None:
                        find_empty_arrays(el.get("pred"), the_id=el["_id"], ids_to_del=ids_to_del)


# Extracts empty grouping containers (eg. an "and" container with no condition inside) and writes their _ids into a list so we can delete them via delete_subdict_by_id
def extract_empty_group_ids(d: dict = None, the_id: int = None, ids_to_del: list = None) -> list:
    if (isinstance(d, dict)) and (d.get("func") is not None):
        if at_least_once_in_dict(key="func", values_whitelist=grouping_functions, dct=d) is False:
            print(f"found empty group: {d}\n")
            ids_to_del.append(d["_id"])
            return
    if d.get("pred") is not None:
        extract_empty_group_ids(d["pred"], the_id=d["pred"]["_id"], ids_to_del=ids_to_del)

    elif d.get("preds") is not None:  # list of elements
        for el in d["preds"]:
            extract_empty_group_ids(el, the_id=el["_id"], ids_to_del=ids_to_del)


# Travels through a segment definition and generates a list of each subdictionary (`components`) and
# a list of alternative segment definitions (`alt_definitions`) where individual components are removed
def slice_up_segment(dfn: dict = None, components: list = None, alt_definitions: list = None,
                     original_seg_wrk: dict = None, iterator: int = None):
    if (dfn.get("pred") is None) and (
            dfn.get("preds") is None):  # base case, no deeper level exists
        print(f"component nr. {iterator} found:")
        print(dumps(dfn, indent=3))

        components.append(dfn)
        iterator += 1
    elif dfn.get("pred") is not None:

        if dfn.get("pred", {}).get(
                "func") in grouping_functions:  # if it is a grouping function (e.g. "and" / "or" / "without" / "container" etc.), ...
            # ... cut out entire subsegment below
            seg_copy = delete_subdict_by_id(copy.deepcopy(original_seg_wrk), dfn["pred"]["_id"])
            # since we cut out an entire sub-segment, it could be that there is nothing else left in the segment,
            # e.g. if there is only one "and" container in the segment and nothing else, that would be an invalid segment.
            # But if there is more, we want to add that as an alternative definition.

            # Check if there is at least one non-grouping function evaluator in the segment left (=it is not just an empty segment anymore):
            if at_least_once_in_dict(key="func", values_whitelist=grouping_functions,
                                     dct=seg_copy['definition']) is True:
                # if we found at least one none-grouping func, we add the seg_copy to the alternative segment definitions
                seg_copy[
                    "name"] = f'Variant {iterator}: {seg_copy["name"]}'
                alt_definitions.append({"seg_def": copy.deepcopy(seg_copy),
                                        "removed_part": dfn["pred"]})
                iterator += 1

        slice_up_segment(dfn.get("pred"), components, alt_definitions, original_seg_wrk,
                         iterator)  # go one level deeper
    elif dfn.get("preds") is not None:  # matryoshka case 4: list of elements (single elements or sub-containers)
        for el in dfn.get("preds"):
            # cut out individual elements (eg each "or" component)
            # we don't need to worry about leaving a preds list with just one element behind. Adobe, when creating the segment,
            # handles this nicely: A one-value "and/or" group e.g. simply becomes a single condition with no and/or group around it.
            seg_copy = delete_subdict_by_id(copy.deepcopy(original_seg_wrk), el["_id"])
            # seg_copy = remove_nones_from_dict(seg_copy)
            seg_copy["name"] = f'Variant {iterator}: {seg_copy["name"]}'
            alt_definitions.append({"seg_def": copy.deepcopy(seg_copy),
                                    "removed_part": el})
            # components.append(dfn)
            iterator += 1
            slice_up_segment(el, components, alt_definitions, original_seg_wrk, iterator)

# Takes an original request `_req` and modifies the segment definition by the `seg_defi` provided to then get the data for that alternative segment
def get_comp_report(seg_defi: dict = None, _req: dict = None):
    if _req["globalFilters"][0].get("segmentId") is not None:
        del _req["globalFilters"][0]["segmentId"]
    # replace with the new segment definition
    _req["globalFilters"][0]["segmentDefinition"] = seg_defi["definition"]
    return ags.getReport2(request=_req).dataframe

# compares the dataframe with the report for of the current segment definition with the data of the alternative segment definition
def compare_data(_comp_data, _current_data):
    curr_metric1 = _current_data[metric_ids[0]].sum()
    comp_metric1 = _comp_data[metric_ids[0]].sum()
    curr_metric2 = _current_data[metric_ids[1]].sum()
    comp_metric2 = _comp_data[metric_ids[1]].sum()
    if curr_metric1 != comp_metric1:
        # if the 2 metrics differ between the two segment definitions
        word = "not identical"
        if comp_metric1 == 0:  # special case: the new segment is empty (= actually also not identical, can be discarded as a solution)
            print("The new segment definition returns no data.")
            return "zero"
    else:
        # if there is no difference for the first metric between the two segment definitions, we check the other metric
        if curr_metric2 == comp_metric2:
            word = "identical"
        else:
            word = f"nearly identical, but {metric_ids[1]} are not"
    print(
        f"The new segment definition ({comp_metric1}) is {word} to the original segment definition ({curr_metric1}).")

    return word


# get the original segment
original_seg = ags.getSegment(segment_id=seg_id, full=True)

original_seg_wrk = copy.deepcopy(original_seg)  # working copy
now = dt.datetime.now()
start_date = now - dt.timedelta(days=days_back)
end_date_str = now.strftime(
    '%Y-%m-%d') + 'T00:00:00.000'  # today at 00.00.00.000 is how the interface does it. I guess data source hits are stored at 00:00:00.000 so the result is not the same as 23:59:59.000
start_date_str = start_date.strftime('%Y-%m-%d') + 'T00:00:00.000'
date_str = f"{start_date_str}/{end_date_str}"
req = {
    "rsid": rs_id,
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": seg_id
        },
        {
            "type": "dateRange",
            "dateRange": date_str,  # "2023-05-03T00:00:00.000/2023-05-10T00:00:00.000",
            "dateRangeId": "5c9760285849420dfc8b406e"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": f"{metric_ids[0]}:::0",
                "id": f"{metric_ids[0]}",
                "filters": [
                    "1"
                ]
            },
            {
                "columnId": f"{metric_ids[1]}",
                "id": f"{metric_ids[1]}",
                "filters": [
                    "1"
                ]
            },

        ],
        "metricFilters": [
            {
                "id": "1",
                "type": "segment",
                "segmentId": "All_Visits"
            }
        ]
    },
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "dimensionSort": "asc"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            }
        ]
    }
}
# query the benchmark report
current_data = ags.getReport2(req).dataframe

current_metric1 = current_data.loc[0, metric_ids[0]]
current_metric2 = current_data.loc[0, metric_ids[1]]

assign_ids_recursive(original_seg_wrk)

defi = original_seg_wrk["definition"]["container"]
# Generating a list of each subdictionary and a list of alternative segment definitions where individual components are removed
components = []
alt_definitions = []
iterator = 1
slice_up_segment(dfn=defi, components=components, alt_definitions=alt_definitions, original_seg_wrk=original_seg_wrk,
                 iterator=iterator)

# remove empty groups
for ind, seg in enumerate(alt_definitions):
    alt_definitions[ind]["seg_def_raw"] = copy.deepcopy(seg["seg_def"])
    ids_to_del = []
    extract_empty_group_ids(seg["seg_def"]["definition"]["container"], ids_to_del=ids_to_del)
    for i in ids_to_del:
        seg["seg_def"]["definition"]["container"] = delete_subdict_by_id(
            seg["seg_def"]["definition"]["container"], i)
    if seg["seg_def"] == seg["seg_def_raw"]:
        print("no empty groups to delete found")
    else:
        print("removed at least one empty group")

# remove duplicate definitions: Removing empty groups can lead to duplicate definitions (e.g. if a "hit" container
# around an empty "and" container is removed, the segment definition without the "and" container will be identical
# to the segment definition with the removed "hit" container)
alt_definitions_to_pop = []
for ind, seg in enumerate(alt_definitions):
    if ind not in alt_definitions_to_pop:
        for ind2, seg2 in enumerate(alt_definitions):
            if ind != ind2:
                if seg["seg_def"]["definition"]["container"] == seg2["seg_def"]["definition"]["container"]:
                    print("duplicate definition found, removing")
                    alt_definitions_to_pop.append(ind2)  # todo ADD THIS TO ARTICLE

for i in sorted(alt_definitions_to_pop, reverse=True):
    alt_definitions.pop(i)

# Now pruning the segment definition, starting with multi-value (contains/equals any of) components
test_seg_tpl = copy.deepcopy(original_seg_wrk)
test_seg_tpl["name"] = f"Test Segment for multi-value component pruning {dt.datetime.now().strftime('%Y%m%d-%H%M%S')}"
test_seg_tpl["definition"]["container"] = {
    "func": "container",
    "context": "hits",
    "pred": {}  # this is filled by each round of the loop
}
# remove the _id key from the test segment definition to make it valid
delete_keys_from_dict(test_seg_tpl, _key="_id")
# in segment definitions, equals any of needs a "," as a separator, contains any of needs a " " as a separator (legacy nonsense)
delimiter_map = {
    "contains-any-of": " ",
    "streq-in": ",",
    "not-contains-any-of": " ",
    "not-streq-in": ","
}
shortened_multival_comps = []
multival_comps = 0
pruned_multival_comps = 0

for comp in components:
    func = comp.get("func", "")
    if func in delimiter_map.keys():  # if it is a multi-value component (eg contains-any-of)
        multival_comps += 1
        var = comp.get("description", comp.get("val", {}).get("name", "no_name"))
        list_len = len(comp.get('list', []))
        if list_len < 2:
            print(
                f"Component for variable {var} has only one value, so we will not treat it like a multi-value "
                f"component. Skipping this component.")
            continue
        print(
            f"Pruning multi-value segment component for '{var}' of type {func} with {list_len} values")
        print(f"Full component to prune: {dumps(comp, indent=2)}")

        if list_len == 0:
            log().error(f"Component of type '{func}' component for variable {var} has no values, which is an "
                        f"invalid segment structure. Skipping this component.")
            continue

        baseline_seg = copy.deepcopy(test_seg_tpl)
        comp_copy = copy.deepcopy(comp)
        baseline_seg["definition"]["container"]["pred"] = copy.deepcopy(comp_copy)
        _id = comp_copy.get("_id", -1)
        if _id == -1:
            raise Exception(f"Component {comp_copy} has no _id!")
        delete_keys_from_dict(baseline_seg, _key="_id")
        print("Getting baseline data = data as per current definition")
        baseline_data = get_comp_report(seg_defi=baseline_seg,
                                        _req=copy.deepcopy(req))
        if baseline_data[metric_ids[0]].sum() == 0:
            print(
                "Component currently returns no data, it probably can be removed entirely (which will be examined in a later check). Skipping it.")
            continue
        _current_metric1 = baseline_data[metric_ids[0]].sum()
        _current_metric2 = baseline_data[metric_ids[1]].sum()

        shortened_multival_comps.append({"old_definition": copy.deepcopy(comp_copy),
                                         "new_definition": copy.deepcopy(comp_copy),
                                         "_id": _id})
        shortened_multival_comps[-1]["new_definition"]["list"] = []  # clear list first
        shortened_multival_comps[-1]["old_definition_str"] = delimiter_map[func].join(
            shortened_multival_comps[-1]["old_definition"]["list"])
        test_seg_tpl["definition"]["container"]["pred"] = copy.deepcopy(comp_copy)
        delete_keys_from_dict(
            test_seg_tpl)  # we are actually evaluating this segment in AA, so the _id keys must go
        # remove duplicates
        comp_copy["list"] = list(set(comp_copy["list"]))
        if len(comp_copy["list"]) < list_len:
            print(f"Removed {list_len - len(comp_copy['list'])} duplicates from component {var}")
        list_len_no_dupes = len(comp_copy["list"])  # update list_len with the new length

        original_list = comp_copy["list"].copy()
        for index, value in enumerate(original_list):
            value_to_test = original_list[index]
            print(f"Testing without value: {value_to_test} (value {index + 1} of {list_len})")
            shorter_list = comp_copy["list"].copy()
            shorter_list.remove(value_to_test)  # [index + 1:]
            test_seg_tpl["definition"]["container"]["pred"]["list"] = shorter_list
            comp_data = get_comp_report(seg_defi=test_seg_tpl, _req=copy.deepcopy(req))
            result = compare_data(comp_data, baseline_data)
            if result == "identical":
                print(f"'{value_to_test}' can be removed from the component without changing the data.")
                comp_copy["list"].remove(value_to_test)
            else:  # keep it
                shortened_multival_comps[-1]["new_definition"]["list"].append(value_to_test)
                print(f"'{value_to_test}' has to stay in the filter.")

            # we are done iterating through the multi-value lists of the component
        print(f"Done pruning the {func} values of component {var}")
        shortened_multival_comps[-1]["new_definition_str"] = delimiter_map[func].join(
            shortened_multival_comps[-1]["new_definition"]["list"])
        shortened_multival_comps[-1]["new_definition"]["_id"] = _id  # re-add the ID
        new_len = len(shortened_multival_comps[-1]["new_definition"]["list"])
        if new_len == list_len:
            print(f"Component {var} could not be pruned, all values are needed.")
            shortened_multival_comps[-1]["pruned"] = False
        else:
            print(f"Component {var} can be pruned from {list_len} to {new_len} values.")
            shortened_multival_comps[-1]["pruned"] = True
            shortened_multival_comps[-1]["pruned_by"] = list_len - new_len
            pruned_multival_comps += 1

if multival_comps > 0:
    print(f"\nThe following changes can be done to multi-value components without changing the data:\n"
               f"{dumps(shortened_multival_comps, indent=3)}")
    multi_value_msg = f"Pruning checks for {multival_comps} multi-value components done. {pruned_multival_comps} " \
                      f"value{'s' if pruned_multival_comps > 1 or pruned_multival_comps < 1 else ''} can be " \
                      f"pruned without changing the data."
    print(multi_value_msg)
else:
    print("No multi-value components found in the segment definition, skipping this step.")

if pruned_multival_comps > 0:
    print(
        "Replacing the original multi-value components by their shortened variants in the segment definition")

    for sc in shortened_multival_comps:
        if not sc["pruned"]:  # if the component could not be pruned, no need to update the original segment
            continue
        the_id = sc["_id"]
        # find the component in the original segment definition by _id and replace it with the shortened version
        original_seg_wrk = replace_subdict_by_id(d=original_seg_wrk, subdict_id=the_id, key="_id",
                                                 replace_by=sc["new_definition"])
        # replace the multi-value component in the alternative segment-1 definitions by its shortened version
        for alt_def in alt_definitions:
            alt_def["seg_def"] = replace_subdict_by_id(d=alt_def["seg_def"], subdict_id=the_id, key="_id",
                                                       replace_by=sc["new_definition"])

print(
    f"Original segment definition after pruning multi-value elements: {dumps(original_seg_wrk, indent=2)}")

# Find non-data-changing alt_definitions
iterator = 0
alt_defs_non_chg = []  # alternative non-data-changing segment definitions
rem_bec_subset = []  # removed because subset of larger, non-data-changing container
for index, dfi in enumerate(alt_definitions):
    print(f"Checking alternative definition {index} of {len(alt_definitions)}.")

    # check if segment is subset of a larger, previously evaluated, non-data-changing container (= part of same_data_but_smaller_definitions)
    # example: AND-container C with 2 Elements:
    # 1. Var X = A
    # 2. Var Y = B
    # If we validated that removing C already brings no change to the data, we don't need to evaluate A and B and can remove them from the definitions to check
    skip = False
    for item_to_check_against in alt_defs_non_chg:
        if dfi == item_to_check_against:  # no need to check the item against itself
            continue
        # check against all items in previously evaluated definitions
        if is_part_of_larger_dict(dfi["removed_part"], item_to_check_against["removed_part"]):
            print(f"removing because it is part of a larger, also non-data-changing container: \n"
                       f"Removed item: {dumps(dfi['removed_part'], indent=3)}. \n"
                       f"Subset of: {dumps(item_to_check_against['removed_part'], indent=3)}")
            rem_bec_subset.append(dfi)
            skip = True  # we skip this validation

    if skip is True:
        continue

    this_dfi_seg = copy.deepcopy(dfi["seg_def"])
    # remove _id keys from segment definitions to pass AA validation
    delete_keys_from_dict(this_dfi_seg)
    print(
        f"Round {iterator}: Validating temp segment '{this_dfi_seg['name']}'.")

    new_seg = ags.createSegmentValidate(segmentJSON=this_dfi_seg)
    if new_seg.get("errorCode") is not None:
        raise Exception(f"Error validating segment: {new_seg}")
    print(f"Segment validated successfully")
    comp_data = get_comp_report(seg_defi=this_dfi_seg, _req=copy.deepcopy(req))
    # compare values to original: if the same, segment component is not needed => will be added to same_data_but_smaller_definitions
    result = compare_data(comp_data, current_data)
    dfi[metric_ids[0]] = comp_data[metric_ids[0]].sum()
    dfi[metric_ids[1]] = comp_data[metric_ids[1]].sum()
    if result == "identical":
        alt_defs_non_chg.append(dfi)

    iterator += 1

print(
    f"Validating {len(alt_definitions)} alternative segment definitions completed. In the process, we did not validate \n"
    f"{len(rem_bec_subset)} parts because they are part of a larger, also non-data-changing container.\n")

len_alt_defs_non_chg = len(alt_defs_non_chg)
print(f"{len_alt_defs_non_chg} alternative, shorter (-1), non-data-changing segment definitions found")

if len_alt_defs_non_chg == 0:
    if pruned_multival_comps == 0:
        print(
            "No alternative segment definitions found where we could remove a component completely without changing the data.")
        exit()

    else:
        print(
            "No alternative segment definitions found where we could remove a component completely without changing the data. "
            f"But we have {pruned_multival_comps} multi-value "
            f"component{'s' if multival_comps > 0 or multival_comps < 1 else ''} that we can try to prune. "
            f"Creating a pruned version of the segment.")
        output_str = "---Summary---\n\n"
        output_str += f"No segment component can be removed entirely. But we could prune {pruned_multival_comps} " \
                      f"multi-value components that we could prune."
        alternative_segment = copy.deepcopy(original_seg_wrk)
        alternative_segment[
            "name"] = f"Pruned Version {dt.datetime.now().strftime('%Y%m%d-%H%M%S')} of: {alternative_segment['name']}"
        delete_keys_from_dict(alternative_segment)
        new_seg = ags.createSegment(segmentJSON=alternative_segment)
        msg = f"Created alternative pruned segment:\n\nName: '{alternative_segment['name']}'\n" \
              f"ID: '{new_seg['id']}'.\n\nTo find the " \
              f"segment, note that the owner is the same user as the owner of the original segment. " \
              f"\n\nTo create the segment, {pruned_multival_comps} multi-value components were found that we " \
              f"could prune without losing any data. We could not find a component however that we could " \
              f"remove entirely from the segment without changing the data."
        print(msg)
        exit()

print(
    f"We identified {len(alt_defs_non_chg)} parts that we could remove from the segment without changing the data.\n"
    f"However, we cannot simply remove all parts. Instead, we need to find out which combinations of these "
    f"parts can be removed without changing the data, starting with the largest possible combinations.")

# Find combinations of non-data-changing elements
# generate combinations of all smaller segment-1 definitions to avoid that 2 or more combinations of each would change the data
# (example: Site Section as eVar = "Home" OR Site Section = "Home" would both not change the data if I remove one, but if I remove both, it does change the data)
# The following line does the following:
# I have:
# A = [1,2,3]
# I get:
# B = [[1], [1,2], [1,2,3], [2], [2,3], [3]]
alt_defs_non_chg_combos = [alt_defs_non_chg[i:j] for i in range(len(alt_defs_non_chg)) for j in
                           range(i + 1, len(alt_defs_non_chg) + 1)]
print(
    f"Generated all possible ({len(alt_defs_non_chg_combos)}) combinations of the parts that we can remove from the segment without changing the data.")
# sort combinations by length of the combination
alt_defs_non_chg_combos.sort(key=len, reverse=True)
print(f"the longest combination has {len(alt_defs_non_chg_combos[0])} parts.")

# change the structure a bit to have a slot for the data results
alt_defs_non_chg_combos_enh = []
for iterator, combo in enumerate(alt_defs_non_chg_combos, start=1):
    alt_defs_non_chg_combos_enh.append({
        "seg_combos": combo,
        "combo_id": iterator
    })

### Create segments that have all the valid combinations removed
pruned_seg_combos = []
for ind, combo_el in enumerate(alt_defs_non_chg_combos_enh):
    original_seg_def_copy = copy.deepcopy(original_seg_wrk["definition"]["container"])
    for seg_part in combo_el["seg_combos"]:
        # remove the removed_part from the original segment:

        original_seg_def_copy = delete_subdict_by_id(copy.deepcopy(original_seg_def_copy),
                                                     seg_part["removed_part"]["_id"])


    # if it is not an empty segment now (edge case where every single component of a segment is non-data-changing)
    if at_least_once_in_dict(key="func", values_whitelist=grouping_functions,
                             dct=original_seg_def_copy) is False:
        continue # empty segment, we can ignore it

    # Searching for now empty containers and deleting them...
    ids_to_del = []
    extract_empty_group_ids(d=original_seg_def_copy, ids_to_del=ids_to_del)
    for __id in ids_to_del:
        original_seg_def_copy = delete_subdict_by_id(original_seg_def_copy, subdict_id=__id, key="_id")
    ids_to_del = []
    # adds the ids with empty arrays to ids_to_del
    find_empty_arrays(d=original_seg_def_copy, ids_to_del=ids_to_del)
    for __id in ids_to_del:
        original_seg_def_copy = delete_subdict_by_id(original_seg_def_copy, subdict_id=__id, key="_id")

    # original_seg_def_copy = remove_nones_from_dict(original_seg_def_copy)
    pruned_seg_combos.append({"seg_def": copy.deepcopy(original_seg_def_copy),
                              "combo_id": ind + 1})  # we want to start with 1, not 0 (used just for logging)

print(
    f"Created {len(pruned_seg_combos)} 'pruned segment' variations (segments without all viable combinations of"
    f" parts which are in themselves not data-changing")

# validate each combination against the data
# since we start with the largest combinations, we can stop if the first combination (all parts) does not change the data
valid_combo = None
req_copy = copy.deepcopy(req)
pruned_seg_combos_copy = copy.deepcopy(pruned_seg_combos)  # debugging

new_seg_ids = []
for index, seg in enumerate(pruned_seg_combos):
    pruned_seg_to_eval = copy.deepcopy(original_seg_wrk)
    pruned_seg_to_eval["definition"]["container"] = copy.deepcopy(seg["seg_def"])
    # remove the "_id"s
    delete_keys_from_dict(pruned_seg_to_eval)
    new_seg = ags.createSegmentValidate(segmentJSON=pruned_seg_to_eval)
    if new_seg.get("errorCode") is not None:
        raise Exception(f"Error validating combo-pruned segment: {new_seg}")

    print(f"Segment validated successfully")
    pruned_seg_to_eval[
        "name"] = f"Pruned Segment {seg['combo_id']}-{dt.datetime.now().strftime('%Y%m%d-%H%M%S')} of: {pruned_seg_to_eval['name']}"
    # for debugging: uncomment to create a segment for each combination
    # new_seg_ids.append(ags.createSegment(segmentJSON=original_seg_copy))
    comp_data = get_comp_report(seg_defi=pruned_seg_to_eval, _req=req_copy)
    # compare values to original: if the same, segment component is not needed => will be added to same_data_but_smaller_definitions
    result = compare_data(comp_data, current_data)
    seg[metric_ids[0]] = comp_data[metric_ids[0]].sum()
    seg[metric_ids[1]] = comp_data[metric_ids[1]].sum()
    if result == "identical":
        print(
            f"Found largest possible non-data-changing combination (index {index}, combo ID {seg['combo_id']}) of parts!")
        valid_combo = {
            "seg_json": pruned_seg_to_eval,
            "combo_id": seg["combo_id"],
            metric_ids[0]: seg[metric_ids[0]],
            metric_ids[1]: seg[metric_ids[1]]
        }
        break
    # otherwise, we try with the next-smallest combination in the list
    iterator += 1

# finalize
if valid_combo is None:
    raise Exception(
        "Something went wrong. We did not find a valid combination of parts that we can remove from the segment without changing the data.")

output_str = "----Summary----\n"
print(
    "The following pruned segment definition is a valid replacement as the data it returns is identical to that of the original segment:")
print(f"{dumps(valid_combo, indent=2)} \n")
output_str += f"\nFound an alternative segment definition where some parts were removed without changing the data the original segment returned.\n"

# create example segment and return link to segment (or Segment ID)
# alternative_segment = copy.deepcopy(original_seg_wrk)
alternative_segment = valid_combo["seg_json"]
alternative_segment["name"] = f"Pruned Version {dt.datetime.now().strftime('%Y%m%d-%H%M%S')} of: {original_seg_wrk['name']}"
new_seg = ags.createSegment(segmentJSON=alternative_segment)
output_str += f"\nCreated alternative segment: \nName: '{alternative_segment['name']}'\n" \
              f"ID: '{new_seg['id']}'. \n\nTo find the " \
              f"segment, note that the owner is the same user as the owner of the original segment."
print(output_str)
