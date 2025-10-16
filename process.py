from utils import print_log
from parsel import Selector
import json
from utils import c_replace, get_useragent, clean_url
import json_repair
import pymongo
import re
from datetime import datetime, UTC
from config import (STORAGE_TYPE, s3_client, SPACES_BUCKET, SPACES_ENDPOINT,
                    db_name, collection_name, collection, pdp_data)

def fb_process(html_response, idd, input_url, html_file_path, country):
    print_log("Parse: building selector tree")
    tree = Selector(text=html_response)

    #todo profile details
    profile_data = tree.xpath('//script[@type="application/json"][contains(text(), "profile_tile_section_type")]/text()').get()
    if profile_data:
        print_log("Parse: branch=PROFILE (profile_tile_section_type found)")
        profile_json = json.loads(profile_data)

        user_data = {}
        try:
            require_data = profile_json["require"][0][3][0]["__bbox"]["require"]
            for entry in require_data:
                try:
                    data = entry[3][1]["__bbox"]["result"]["data"]

                    # Case 1: data has "user"
                    if "user" in data and "profile_tile_sections" in data["user"]:
                        user_data = data["user"]
                        break

                    # Case 2: data directly has "profile_tile_sections"
                    elif "profile_tile_sections" in data:
                        user_data = data
                        break

                except (KeyError, IndexError, TypeError):
                    continue
        except (KeyError, IndexError, TypeError):
            user_data = {}

        # todo category
        try:
            profile_category = [
                item["node"]["timeline_context_item"]["renderer"]["context_item"]["title"]["text"]
                for section in user_data.get("profile_tile_sections", {}).get("edges", [])
                for view_node in section.get("node", {}).get("profile_tile_views", {}).get("nodes", [])
                for item in
                (view_node.get("view_style_renderer", {}) or {}).get("view", {}).get("profile_tile_items", {}).get("nodes",
                                                                                                                    [])
                if item.get("node", {}).get("timeline_context_item", {}).get(
                    "timeline_context_list_item_type") == "INTRO_CARD_INFLUENCER_CATEGORY"
            ]
        except:
            profile_category = ''
        profile_category = profile_category[0] if profile_category else None
        if profile_category:
            profile_category = c_replace(profile_category)
            if profile_category.lower().startswith("page ·"):
                profile_category = profile_category[6:].strip()

        #todo address
        try:
            profile_address = [
                item["node"]["timeline_context_item"]["renderer"]["context_item"]["title"]["text"]
                for section in user_data.get("profile_tile_sections", {}).get("edges", [])
                for view_node in section.get("node", {}).get("profile_tile_views", {}).get("nodes", [])
                for item in
                (view_node.get("view_style_renderer", {}) or {}).get("view", {}).get("profile_tile_items", {}).get("nodes",
                                                                                                                    [])
                if item.get("node", {}).get("timeline_context_item", {}).get(
                    "timeline_context_list_item_type") == "INTRO_CARD_ADDRESS"
            ]
        except:
            profile_address = ''

        if not profile_address:
            try:
                profile_address = [
                    item["node"]["timeline_context_item"]["renderer"]["context_item"]["title"]["text"]
                    for section in user_data.get("profile_tile_sections", {}).get("edges", [])
                    for view_node in section.get("node", {}).get("profile_tile_views", {}).get("nodes", [])
                    for item in
                    (view_node.get("view_style_renderer", {}) or {}).get("view", {}).get("profile_tile_items",
                                                                                            {}).get("nodes",
                                                                                                    [])
                    if item.get("node", {}).get("timeline_context_item", {}).get(
                        "timeline_context_list_item_type") == "INTRO_CARD_BUSINESS_SERVICE_AREA"
                ]
            except:
                profile_address = ''


        if profile_address:
            profile_address = c_replace(profile_address[0] if profile_address else None)
            profile_address = profile_address.replace("\n", "")
            profile_address = ' '.join(profile_address.split())
        else:
            profile_address = ''

        #todo contact info
        try:
            profile_contact = [
                item["node"]["timeline_context_item"]["renderer"]["context_item"]["title"]["text"]
                for section in user_data.get("profile_tile_sections", {}).get("edges", [])
                for view_node in section.get("node", {}).get("profile_tile_views", {}).get("nodes", [])
                for item in
                (view_node.get("view_style_renderer", {}) or {}).get("view", {}).get("profile_tile_items", {}).get("nodes",
                                                                                                                    [])
                if item.get("node", {}).get("timeline_context_item", {}).get(
                    "timeline_context_list_item_type") == "INTRO_CARD_PROFILE_PHONE"
            ]
        except:
            profile_contact = ''
        profile_contact = profile_contact[0] if profile_contact else None

        #todo email
        try:
            mail_info = [
                item["node"]["timeline_context_item"]["renderer"]["context_item"]["title"]["text"]
                for section in user_data.get("profile_tile_sections", {}).get("edges", [])
                for view_node in section.get("node", {}).get("profile_tile_views", {}).get("nodes", [])
                for item in
                (view_node.get("view_style_renderer", {}) or {}).get("view", {}).get("profile_tile_items", {}).get("nodes",
                                                                                                                    [])
                if item.get("node", {}).get("timeline_context_item", {}).get(
                    "timeline_context_list_item_type") == "INTRO_CARD_PROFILE_EMAIL"
            ]
        except:
            mail_info = ''
        mail_info = mail_info[0] if mail_info else None

        # todo website
        try:
            website_info = [
                item["node"]["timeline_context_item"]["renderer"]["context_item"]["title"]["text"]
                for section in user_data.get("profile_tile_sections", {}).get("edges", [])
                for view_node in section.get("node", {}).get("profile_tile_views", {}).get("nodes", [])
                for item in(view_node.get("view_style_renderer", {}) or {}).get("view", {}).get("profile_tile_items", {}).get("nodes",[])
                if item.get("node", {}).get("timeline_context_item", {}).get("timeline_context_list_item_type") == "INTRO_CARD_WEBSITE"]
        except:
            website_info = ''

        profile_website1 = clean_url( website_info[0] if len(website_info) > 0 else None)
        profile_website2 = clean_url( website_info[1] if len(website_info) > 1 else None)
        profile_website3 = clean_url( website_info[2] if len(website_info) > 2 else None)

        #TODO profile

        try:
            bio_text = next(
                (
                    item.get("node", {}).get("profile_status_text", {}).get("text")
                    for section in user_data.get("profile_tile_sections", {}).get("edges", [])
                    for view_node in section.get("node", {}).get("profile_tile_views", {}).get("nodes", [])
                    if
                view_node.get("view_style_renderer", {}).get("__typename") == "ProfileTileViewIntroBioRenderer"
                    for item in
                view_node.get("view_style_renderer", {}).get("view", {}).get("profile_tile_items", {}).get(
                    "nodes", [])
                    if item.get("node", {}).get("__typename") == "ProfileStatus"
                ),
                None
            )
        except Exception as e:
            bio_text = None

        if bio_text:
            bio_text = c_replace(bio_text)

        #todo profile social context
        try:
            profile_social_data = tree.xpath('//*[contains(text(),"profile_social_context")]/text()').get()
        except:
            profile_social_data = ''
        profile_social_json = json.loads(profile_social_data)


        #todo fb profile name
        fb_profile_name = ''
        try:
            require_data = profile_social_json["require"][0][3][0]["__bbox"]["require"]
            for entry in require_data:
                try:
                    data = entry[3][1]["__bbox"]["result"]["data"]
                    user = data.get("user", {})
                    name = (
                        user.get("profile_header_renderer", {})
                        .get("user", {})
                        .get("name"))
                    if name:
                        fb_profile_name = name
                        break
                except (KeyError, IndexError, TypeError):
                    continue
        except Exception:
            fb_profile_name = ''


        #todo - followers count data
        fb_followers_count = ''

        try:
            require_data = profile_social_json["require"][0][3][0]["__bbox"]["require"]
            for entry in require_data:
                try:
                    data = entry[3][1]["__bbox"]["result"]["data"]
                    user = data.get("user", {})
                    context = (
                        user.get("profile_header_renderer", {})
                        .get("user", {})
                        .get("profile_social_context", {})
                        .get("content", [])
                    )
                    if context and isinstance(context, list):
                        for item in context:
                            text_block = item.get("text", {}).get("text", "")
                            if "followers" in text_block.lower() or "follower" in text_block.lower():
                                fb_followers_count = text_block.lower().replace(" followers", "").replace(' follower','').strip()
                                break  # Stop after finding the first 'followers' entry

                        if fb_followers_count:  # If we already found it, stop outer loop
                            break

                except (KeyError, IndexError, TypeError):
                    continue
        except Exception:
            fb_followers_count = ''

        if fb_followers_count:
            fb_followers_count = fb_followers_count.lower().strip().replace(",", "").replace(" ", "")

            if fb_followers_count.endswith("k"):
                fb_followers_count = int(float(fb_followers_count[:-1]) * 1000)
            elif fb_followers_count.endswith("m"):
                fb_followers_count = int(float(fb_followers_count[:-1]) * 1000000)
            elif fb_followers_count.endswith("b"):
                fb_followers_count = int(float(fb_followers_count[:-1]) * 1000000000)
            else:
                fb_followers_count = int(fb_followers_count)

        #todo facebook url

        fb_url = ''

        try:
            require_data = profile_social_json["require"][0][3][0]["__bbox"]["require"]
            for entry in require_data:
                try:
                    data = entry[3][1]["__bbox"]["result"]["data"]
                    user = data.get("user", {})
                    fb_url = (
                        user.get("profile_header_renderer", {})
                        .get("user", {})
                        .get("url", "")
                    )
                    if fb_url:
                        break
                except (KeyError, IndexError, TypeError):
                    continue
        except Exception:
            fb_url = ''

        page_type = ''

        try:
            require_data = profile_social_json["require"][0][3][0]["__bbox"]["require"]
            for entry in require_data:
                try:
                    data = entry[3][1]["__bbox"]["result"]["data"]
                    user = data.get("user", {})
                    verified = (
                        user.get("profile_header_renderer", {})
                        .get("user", {})
                        .get("show_verified_badge_on_profile")
                    )
                    if isinstance(verified, bool):
                        page_type = 'Official Page' if verified else 'Unofficial Page'
                        break
                except (KeyError, IndexError, TypeError):
                    continue
        except Exception:
            page_type = ''

        # todo post json
        try:
            post_main_data = tree.xpath('//*[contains(text(),"timeline_list_feed_units")]/text()').get()
        except:
            post_main_data = ''

        try:
            post_main_json = json_repair.loads(post_main_data)
        except:
            post_main_json = ''
        #todo last post creation time - regex

        try:
            last_post_creation = re.findall(r'"creation_time":(.*?),', html_response)[0]
            last_post_creation = int(last_post_creation)
            last_post_creation = datetime.fromtimestamp(last_post_creation, UTC).strftime("%Y-%m-%d")
        except:
            last_post_creation = ''

        if fb_profile_name == '':
            try:
                fb_profile_name = tree.xpath('//title//text()').get().split(' | ')[0]
            except:
                fb_profile_name = ''

        print_log("Mongo: inserting parsed data (branch=PROFILE)")

        records ={
            'input_url':input_url,
            'time_stamp':datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            'fb_last_post_date':last_post_creation,
            'fb_url':fb_url.replace("\\/", "/"),
            'fb_url_type':page_type,
            'fb_number_of_followers':fb_followers_count,
            'fb_company_name':fb_profile_name,
            'fb_company_intro':bio_text,
            'fb_category':profile_category,
            'fb_address':profile_address,
            'fb_phone_number':profile_contact,
            'fb_email_address':mail_info,
            'fb_website':profile_website1,
            'fb_website2':profile_website2,
            'fb_website3':profile_website3,
            'hash_id':idd,
            'pagesave':html_file_path,
            'country': country

        }
        try:
            pdp_data.insert_one(records)  # `records` can be a dict (single doc) or list (multiple docs)
            print_log("Mongo: ✅ Data inserted into output collection")
        except pymongo.errors.DuplicateKeyError:
            print_log("Mongo: ⚠️ Duplicate key (already inserted)")
        except Exception as e:
            print_log(f"Mongo: ❌ Insert failed -> {e}")

        collection.update_one({'url_id':idd}, {'$set': {'status': 'done'}})
        # continue

    else:
        profile_data_2 = tree.xpath('//*[contains(text(),"full_address")]//text()').get()
        if profile_data_2:
            profile_json_2 = json.loads(profile_data_2)

            # todo full address
            full_address = ''

            try:
                require_data = profile_json_2["require"][0][3][0]["__bbox"]["require"]
                for entry in require_data:
                    try:
                        data = entry[3][1]["__bbox"]["result"]["data"]
                        if "page" in data:
                            full_address = data["page"]["comet_page_cards"][0]["page"]["page_about_fields"]["address"][
                                "full_address"]
                            break
                    except (KeyError, IndexError, TypeError):
                        continue
            except Exception:
                full_address = ''

            if full_address:
                full_address = c_replace(full_address.replace('\n', ''))


            # todo contact no
            contact_no = ''

            try:
                require_data = profile_json_2["require"][0][3][0]["__bbox"]["require"]
                for entry in require_data:
                    try:
                        data = entry[3][1]["__bbox"]["result"]["data"]
                        if "page" in data:
                            contact_no = data["page"]["comet_page_cards"][0]["page"]["page_about_fields"][
                                "formatted_phone_number"]
                            break
                    except (KeyError, IndexError, TypeError):
                        continue
            except Exception:
                contact_no = ''

            # todo website
            website = ''

            try:
                require_data = profile_json_2["require"][0][3][0]["__bbox"]["require"]
                for entry in require_data:
                    try:
                        data = entry[3][1]["__bbox"]["result"]["data"]
                        if "page" in data:
                            website = data["page"]["comet_page_cards"][0]["page"]["page_about_fields"]["website"]
                            break
                    except (KeyError, IndexError, TypeError):
                        continue
            except Exception:
                website = ''

            # todo description
            fb_description = ''

            try:
                require_data = profile_json_2["require"][0][3][0]["__bbox"]["require"]
                for entry in require_data:
                    try:
                        data = entry[3][1]["__bbox"]["result"]["data"]
                        if "page" in data:
                            fb_description = \
                            data["page"]["comet_page_cards"][0]["page"]["page_about_fields"]["description"]['text']
                            # require[0][3][0].__bbox.require[7][3][1].__bbox.result.data.page.comet_page_cards[
                            #     0].page.page_about_fields.description.text
                            break
                    except (KeyError, IndexError, TypeError):
                        continue
            except Exception:
                fb_description = ''

            if fb_description:
                fb_description = c_replace(fb_description.replace('\n', ''))

            # todo followers count
            followers_count = ''

            try:
                require_data = profile_json_2["require"][0][3][0]["__bbox"]["require"]
                for entry in require_data:
                    try:
                        data = entry[3][1]["__bbox"]["result"]["data"]
                        if "page" in data:
                            followers_count = data["page"]["comet_page_cards"][0]["page"]["follower_count"]
                            break
                    except (KeyError, IndexError, TypeError):
                        continue
            except Exception:
                followers_count = ''


            # todo fb emailaddress
            fb_emailaddress = ''

            try:
                require_data = profile_json_2["require"][0][3][0]["__bbox"]["require"]
                for entry in require_data:
                    try:
                        data = entry[3][1]["__bbox"]["result"]["data"]
                        if "page" in data:
                            fb_emailaddress = data["page"]["comet_page_cards"][0]["page"]["page_about_fields"][
                                "email_address"]
                            break
                    except (KeyError, IndexError, TypeError):
                        continue
            except Exception:
                fb_emailaddress = ''

            if fb_emailaddress:
                fb_emailaddress = clean_url(fb_emailaddress)


            # todo post json
            try:
                post_main = tree.xpath('//*[contains(text(),"creation_time")]/text()').get()
            except:
                post_main = ''
            try:
                post_json = json_repair.loads(post_main)
            except:
                post_json = ''
            # print(post_json)

            try:
                try:
                    last_post_creation_time = \
                    post_json['require'][0][3][0]['__bbox']['require'][83][3][1]['__bbox']['result']['data']['page'][
                        'stories_about_place']['edges'][0]['node']['comet_sections']['content']['story'][
                        'comet_sections']['attached_story']['story']['attached_story']['comet_sections'][
                        'attached_story_layout']['story']['comet_sections']['metadata'][0]['story']['creation_time']
                except:
                    last_post_creation_time = \
                    post_json['require'][0][3][0]['__bbox']['require'][96][3][1]['__bbox']['result']['data']['page'][
                        'stories_about_place']['edges'][0]['node']['comet_sections']['context_layout']['story'][
                        'comet_sections']['metadata'][0]['story'][
                        'creation_time']  # require[0][3][0].__bbox.require[96][3][1].__bbox.result.data.page.stories_about_place.edges[0].node.comet_sections.context_layout.story.comet_sections.metadata[0].story.creation_time

                last_post_creation_time = int(last_post_creation_time)
                last_post_creation_time = datetime.fromtimestamp(last_post_creation_time, UTC).strftime("%Y-%m-%d")
            except:
                last_post_creation_time = ''

            # todo - regex last_post_creation_time
            try:
                last_post_creation_time = re.match(r'"creation_time": (.*?),', html_response)
                if last_post_creation_time:
                    last_post_creation_time = re.group(0)
                else:
                    try:
                        last_post_creation_time = re.findall(r'"creation_time":(.*?),', html_response)[0]
                        last_post_creation_time = int(last_post_creation_time)
                        last_post_creation_time = datetime.fromtimestamp(last_post_creation_time, UTC).strftime(
                            "%Y-%m-%d")
                    except:
                        last_post_creation_time = ""
            except Exception as e:
                print("Error in regex:", e)

            # todo profile_url and category and verification
            profile_data_2_profile = tree.xpath('//*[contains(text(),"uri_token")]//text()').get()
            if profile_data_2_profile:
                profile_data_2_profile = json.loads(profile_data_2_profile)
                verification = ''
                try:
                    require_data = profile_data_2_profile.get("require", [])[0][3][0].get("__bbox", {}).get(
                        "require", [])
                    for entry in require_data:
                        try:
                            data = entry[3][1].get("__bbox", {}).get("result", {}).get("data", {})
                            page_data = data.get("page", {})
                            if "is_verified" in page_data:
                                verification = page_data["is_verified"]
                                break
                        except (KeyError, IndexError, TypeError, AttributeError):
                            continue

                except (KeyError, IndexError, TypeError, AttributeError):
                    verification = ''

                if verification == False:
                    verification = 'Unofficial Page'
                else:
                    verification = 'Official Page'

                profile_url = ''
                try:
                    require_data = profile_data_2_profile.get("require", [])[0][3][0].get("__bbox", {}).get(
                        "require",
                        [])
                    for entry in require_data:
                        try:
                            data = entry[3][1].get("__bbox", {}).get("result", {}).get("data", {})
                            page_data = data.get("page", {})
                            if "url" in page_data:
                                profile_url = page_data["url"]
                                break
                        except (KeyError, IndexError, TypeError, AttributeError):
                            continue

                except (KeyError, IndexError, TypeError, AttributeError):
                    profile_url = ''

                category = ''
                try:
                    require_data = profile_data_2_profile.get("require", [])[0][3][0].get("__bbox", {}).get(
                        "require",
                        [])
                    for entry in require_data:
                        try:
                            data = entry[3][1].get("__bbox", {}).get("result", {}).get("data", {})
                            page_data = data.get("page", {})
                            if "category_name" in page_data:
                                category = page_data["category_name"]
                                break
                        except (KeyError, IndexError, TypeError, AttributeError):
                            continue

                except (KeyError, IndexError, TypeError, AttributeError):
                    category = ''

                if category:
                    category = c_replace(category.replace('page ·', ''))

                fb_name = ''
                try:
                    require_data = profile_data_2_profile.get("require", [])[0][3][0].get("__bbox", {}).get(
                        "require",
                        [])
                    for entry in require_data:
                        try:
                            data = entry[3][1].get("__bbox", {}).get("result", {}).get("data", {})
                            page_data = data.get("page", {})
                            if "name" in page_data:
                                fb_name = page_data["name"]
                                break
                        except (KeyError, IndexError, TypeError, AttributeError):
                            continue

                except (KeyError, IndexError, TypeError, AttributeError):
                    fb_name = ''


                if fb_name == '':
                    try:
                        fb_name = tree.xpath('//title//text()').get().split(' | ')[0]
                    except:
                        fb_name = ''

                records = {
                    'input_url': input_url,
                    'time_stamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                    'fb_last_post_date': last_post_creation_time,
                    'fb_url': profile_url.replace("\\/", "/"),
                    'fb_url_type': verification,
                    'fb_number_of_followers': followers_count,
                    'fb_company_name': fb_name,
                    'fb_company_intro': fb_description,
                    'fb_category': category,
                    'fb_address': full_address,
                    'fb_phone_number': contact_no,
                    'fb_email_address': fb_emailaddress,
                    'fb_website': website,
                    'fb_website2': '',
                    'fb_website3': '',
                    'hash_id': idd,
                    'pagesave': html_file_path,
                    'country': country

                }
                try:
                    pdp_data.insert_one(records)  # `records` can be a dict (single doc) or list (multiple docs)
                    print("Data inserted successfully")
                except pymongo.errors.DuplicateKeyError:
                    print("Data already exists (duplicate key error)")
                except Exception as e:
                    print("Data not inserted:", e)

                collection.update_one({'url_id': idd}, {'$set': {'status': 'done'}})

        else:
            profile_data_3 = tree.xpath('//*[contains(text(),"follower_count")]//text()').get()
            if profile_data_3:
                profile_data_3 = json.loads(profile_data_3)

                page_followers_count = ''

                try:
                    require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                    for entry in require_data:
                        try:
                            data = entry[3][1]["__bbox"]["result"]["data"]
                            if "page" in data:
                                page_followers_count = data["page"]["comet_page_cards"][0]["page"]['follower_count']
                                break
                        except (KeyError, IndexError, TypeError):
                            continue
                except Exception:
                    page_followers_count = ''

                if page_followers_count == '':
                    try:
                        require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    for card in data["page"].get("comet_page_cards", []):
                                        try:
                                            page_followers_count = card["page"]["follower_count"]
                                            break
                                        except (KeyError, TypeError):
                                            continue
                                if page_followers_count:
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_followers_count = ''


                page_email_address = ''

                try:
                    require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                    for entry in require_data:
                        try:
                            data = entry[3][1]["__bbox"]["result"]["data"]
                            if "page" in data:
                                page_email_address = data["page"]["comet_page_cards"][0]["page"]['page_about_fields'][
                                    'email']
                                break
                        except (KeyError, IndexError, TypeError):
                            continue
                except Exception:
                    page_email_address = ''
                if page_email_address == '':
                    try:
                        require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    for card in data["page"].get("comet_page_cards", []):
                                        try:
                                            page_email_address = card["page"]["page_about_fields"]['email']
                                            break
                                        except (KeyError, TypeError):
                                            continue
                                if page_email_address:
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_email_address = ''

                page_fb_address = ''

                try:
                    require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                    for entry in require_data:
                        try:
                            data = entry[3][1]["__bbox"]["result"]["data"]
                            if "page" in data:
                                page_fb_address = data["page"]["comet_page_cards"][0]["page"]['page_about_fields'][
                                    'address']
                                break
                        except (KeyError, IndexError, TypeError):
                            continue
                except Exception:
                    page_fb_address = ''
                if page_fb_address == '':
                    try:
                        require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    for card in data["page"].get("comet_page_cards", []):
                                        try:
                                            page_fb_address = card["page"]["page_about_fields"]['address']
                                            break
                                        except (KeyError, TypeError):
                                            continue
                                if page_fb_address:
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_fb_address = ''

                if page_fb_address:
                    page_fb_address = c_replace(page_fb_address)

                page_contact_no = ''

                try:
                    require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                    for entry in require_data:
                        try:
                            data = entry[3][1]["__bbox"]["result"]["data"]
                            if "page" in data:
                                page_contact_no = data["page"]["comet_page_cards"][0]["page"]['page_about_fields'][
                                    'formatted_phone_number']
                                break
                        except (KeyError, IndexError, TypeError):
                            continue
                except Exception:
                    page_contact_no = ''

                if page_contact_no == '':
                    try:
                        require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    for card in data["page"].get("comet_page_cards", []):
                                        try:
                                            page_contact_no = card["page"]["page_about_fields"][
                                                'formatted_phone_number']
                                            break
                                        except (KeyError, TypeError):
                                            continue
                                if page_contact_no:
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_contact_no = ''
                    # require[0][3][0].__bbox.require[7][3][1].__bbox.result.data.page.comet_page_cards[0].page.page_about_fields.formatted_phone_number

                page_website = ''

                try:
                    require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                    for entry in require_data:
                        try:
                            data = entry[3][1]["__bbox"]["result"]["data"]
                            if "page" in data:
                                page_website = data["page"]["comet_page_cards"][0]["page"]['page_about_fields'][
                                    'website']
                                break
                        except (KeyError, IndexError, TypeError):
                            continue
                except Exception:
                    page_website = ''
                if page_website == '':
                    try:
                        require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    for card in data["page"].get("comet_page_cards", []):
                                        try:
                                            page_website = card["page"]["page_about_fields"]['website']
                                            break
                                        except (KeyError, TypeError):
                                            continue
                                if page_website:
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_website = ''

                    # require[0][3][0].__bbox.require[7][3][1].__bbox.result.data.page.comet_page_cards[0].page.page_about_fields.website
                if page_website:
                    page_website = clean_url(page_website)

                page_description = ''

                try:
                    require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                    for entry in require_data:
                        try:
                            data = entry[3][1]["__bbox"]["result"]["data"]
                            if "page" in data:
                                page_description = \
                                    data["page"]["comet_page_cards"][0]["page"]['page_about_fields']['description'][
                                        'text']
                                break
                        except (KeyError, IndexError, TypeError):
                            continue
                except Exception:
                    page_description = ''
                if page_description == '':
                    try:
                        require_data = profile_data_3["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    for card in data["page"].get("comet_page_cards", []):
                                        try:
                                            page_description = card["page"]["page_about_fields"]['description']
                                            break
                                        except (KeyError, TypeError):
                                            continue
                                if page_description:
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_description = ''

                if page_description:
                    page_description = c_replace(page_description)

                # todo profile data 3 post information
                try:
                    post_main = tree.xpath('//*[contains(text(),"creation_time")]/text()').get()
                except:
                    post_main = ''
                try:
                    post_json = json_repair.loads(post_main)
                except:
                    post_json = ''
                profile_last_post_creation_time = ''
                try:
                    profile_last_post_creation_time = re.match(r'"creation_time": (.*?),', html_response)
                    if profile_last_post_creation_time:
                        profile_last_post_creation_time = re.group(0)
                    else:
                        try:
                            profile_last_post_creation_time = re.findall(r'"creation_time":(.*?),', html_response)[0]
                            profile_last_post_creation_time = int(profile_last_post_creation_time)
                            profile_last_post_creation_time = datetime.fromtimestamp(profile_last_post_creation_time,
                                                                                     UTC).strftime("%Y-%m-%d")
                        except:
                            profile_last_post_creation_time = ""
                except Exception as e:
                    print("Error in regex:", e)


                # todo page url
                page_data_4 = tree.xpath('//*[contains(text(),"verification_status")]//text()').get()
                if page_data_4:
                    page_data_4_json = json.loads(page_data_4)

                    page_url = ''
                    try:
                        require_data = page_data_4_json["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    page_url = data["page"]['url']
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                                # require[0][3][0].__bbox.require[5][3][1].__bbox.result.data.page.comet_page_cards[0].page.page_about_fields.page_categories[0].text
                    except Exception:
                        page_url = ''

                    # todo page name
                    page_name = ''
                    try:
                        require_data = page_data_4_json["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    page_name = data["page"]["name"]
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                                # require[0][3][0].__bbox.require[3][3][1].__bbox.result.data.page.name
                    except Exception:
                        page_name = ''

                    # todo page category
                    page_category = ''
                    try:
                        require_data = page_data_4_json["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    page_category = data["page"]['category_name']
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_category = ''

                    # todo page verification
                    page_verification = ''
                    try:
                        require_data = page_data_4_json["require"][0][3][0]["__bbox"]["require"]
                        for entry in require_data:
                            try:
                                data = entry[3][1]["__bbox"]["result"]["data"]
                                if "page" in data:
                                    page_verification = data["page"]['verification_status']
                                    break
                            except (KeyError, IndexError, TypeError):
                                continue
                    except Exception:
                        page_verification = ''

                    if page_verification == 'NOT_VERIFIED':
                        page_verification = 'Unofficial Page'
                    else:
                        page_verification = 'Official Page'

                    if page_name == '':
                        try:
                            page_name = tree.xpath('//title//text()').get().split(' | ')[0]
                        except:
                            page_name = ''
                    records = {
                        'input_url': input_url,
                        'time_stamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                        'fb_last_post_date': profile_last_post_creation_time,
                        'fb_url': page_url.replace("\\/", "/"),
                        'fb_url_type': page_verification,
                        'fb_number_of_followers': page_followers_count,
                        'fb_company_name': page_name,
                        'fb_company_intro': page_description,
                        'fb_category': page_category,
                        'fb_address': page_fb_address,
                        'fb_phone_number': page_contact_no,
                        'fb_email_address': page_email_address,
                        'fb_website': page_website,
                        'fb_website2': '',
                        'fb_website3': '',
                        'hash_id': idd,
                        'pagesave': html_file_path,
                        'country': country
                    }
                    try:
                        pdp_data.insert_one(records)  # `records` can be a dict (single doc) or list (multiple docs)
                        print("Data inserted successfully")
                    except pymongo.errors.DuplicateKeyError:
                        print("Data already exists (duplicate key error)")
                    except Exception as e:
                        print("Data not inserted:", e)

                    collection.update_one({'url_id': idd}, {'$set': {'status': 'done'}})

