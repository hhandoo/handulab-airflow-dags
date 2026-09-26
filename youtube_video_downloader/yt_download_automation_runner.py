from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

from datetime import datetime
from pathlib import Path

import os
import re
import shutil
import subprocess
import yt_dlp

from yt_dlp.utils import download_range_func


# ============================================================
# Helper functions
# ============================================================

def timestamp_to_seconds(timestamp: str) -> int:
    """
    Convert HH:MM:SS into seconds.
    """
    if not re.fullmatch(r"\d{2}:\d{2}:\d{2}", timestamp):
        raise ValueError(
            f"Invalid timestamp '{timestamp}'. "
            "Expected format HH:MM:SS"
        )

    h, m, s = map(int, timestamp.split(":"))

    if m >= 60 or s >= 60:
        raise ValueError(
            f"Invalid timestamp '{timestamp}'. "
            "Minutes and seconds must be less than 60."
        )

    return h * 3600 + m * 60 + s


def get_video_id(url: str) -> str:
    """
    Extract the 11-character YouTube video ID.
    """

    patterns = [
        r"(?:v=)([A-Za-z0-9_-]{11})",
        r"(?:youtu\.be/)([A-Za-z0-9_-]{11})",
        r"(?:youtube\.com/shorts/)([A-Za-z0-9_-]{11})",
        r"(?:youtube\.com/embed/)([A-Za-z0-9_-]{11})",
    ]

    for pattern in patterns:
        match = re.search(pattern, url)

        if match:
            return match.group(1)

    raise ValueError(
        f"Could not extract YouTube video ID from URL: {url}"
    )


def timestamp_for_youtube_url(timestamp: str) -> str:
    """
    Convert HH:MM:SS into YouTube timestamp format.

    Example:
        01:02:20 -> 1h02m20s
    """

    h, m, s = map(int, timestamp.split(":"))

    return f"{h}h{m:02d}m{s:02d}s"


def safe_timestamp_for_directory(timestamp: str) -> str:
    """
    Convert HH:MM:SS to a filesystem-safe format.

    Example:
        01:17:27 -> 01_17_27
    """

    return timestamp.replace(":", "_")


def create_chatgpt_prompts(
    video_context: str,
    video_url: str,
    end_time: str,
) -> str:
    """
    Generate the two ChatGPT prompts that will be saved
    into the output directory.
    """

    video_id = get_video_id(video_url)

    youtube_timestamp = timestamp_for_youtube_url(end_time)

    attribution_url = (
        f"https://www.youtube.com/watch?v={video_id}"
        f"&t={youtube_timestamp}"
    )

    prompt_1 = f"""
============================================================
PROMPT 1 — YOUTUBE TITLE, DESCRIPTION & TAGS
============================================================

You are an expert YouTube growth, SEO, CTR and audience-retention
copywriter.

Analyze the following video context:

{video_context}

Original YouTube video:
{video_url}

The extracted clip ends at:
{end_time}

The attribution URL that MUST appear at the very top of the
YouTube description is:

{attribution_url}

Create optimized YouTube upload metadata for this clip.

Requirements:

1. Generate a highly clickable, high-CTR YouTube title.
2. Make the title suitable for YouTube search and recommendations.
3. Make it attention-grabbing without inventing facts that are not
   supported by the provided context.
4. Generate a compelling YouTube description.
5. The FIRST line of the description must be the attribution to
   the original upload using exactly this URL:

   {attribution_url}

6. The description should clearly provide context about the clip.
7. Make the description engaging and natural rather than sounding
   AI-generated.
8. Do not falsely claim something happened if it is not supported
   by the context.
9. Generate YouTube tags.
10. Tags MUST be comma-separated on ONE SINGLE LINE.
11. Tags MUST be 500 characters or fewer.
12. Optimize the tags for search discovery, related videos,
    audience relevance and YouTube SEO.
13. Avoid generic AI-looking tags.
14. Use natural phrases that a real YouTube creator would enter.

Return ONLY this structure:

TITLE:
<one optimized title>

DESCRIPTION:
<optimized description>

TAGS:
<tag1, tag2, tag3, ...>
"""


    prompt_2 = f"""
============================================================
PROMPT 2 — HIGH-CTR YOUTUBE THUMBNAIL
============================================================

You are an expert YouTube thumbnail designer specializing in
high-CTR debate, commentary and discussion videos.

Video context:

{video_context}

Create a detailed image-generation prompt for a YouTube thumbnail.

THUMBNAIL REQUIREMENTS:

1. Exact canvas size: 1280x720 pixels.
2. 16:9 YouTube thumbnail composition.
3. The thumbnail should immediately communicate the main conflict
   or debate from the provided context.
4. A Person wearing a mask must be positioned on the LEFT side.
5. Person should be shown wearing his recognizable mask.
6. The DEBATER must be positioned on the RIGHT side.
7. Use the video context to determine who the debater is.
8. The two people should have strong contrasting expressions,
   body language and visual tension appropriate to the debate.
9. Make both faces/subjects large and clearly visible on mobile.
10. Use dramatic but realistic lighting.
11. Use strong visual separation between the two sides.
12. Use a clean, professional YouTube thumbnail composition.
13. Any thumbnail text must be short, bold and readable on mobile.
14. Do not overcrowd the thumbnail with text.
15. Do not create unnecessary logos, watermarks or UI elements.
16. Do not copy an existing thumbnail exactly.
17. The design should look like a professionally created
    high-performing YouTube debate thumbnail.
18. Keep important faces and text away from the extreme edges.
19. Prioritize visual storytelling and curiosity.
20. Use only information supported by the supplied context.

IMPORTANT:
Do not invent the identity of the debater if the context does
not provide it. If the debater's identity is present in the
context, use that person as the right-side subject.

Return ONE complete image-generation prompt suitable for an
AI image generator.

The final generated image must be exactly:

1280 x 720 pixels
"""


    return (
        f"VIDEO CONTEXT\n"
        f"===============\n"
        f"{video_context}\n\n"
        f"VIDEO URL\n"
        f"===============\n"
        f"{video_url}\n\n"
        f"END TIME\n"
        f"===============\n"
        f"{end_time}\n\n"
        f"ATTRIBUTION URL\n"
        f"===============\n"
        f"{attribution_url}\n\n\n"
        f"{prompt_1}\n\n"
        f"{prompt_2}\n"
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="youtube_clip_generator",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "yt-dlp",
        "ffmpeg",
        "video",
        "content-generation",
    ],

    params={

        # ----------------------------------------------------
        # Disclaimer / intro video
        # ----------------------------------------------------

        "disclaimer_location": Param(
            default="",
            type="string",
            title="Disclaimer Video Location",
            description=(
                "Full filesystem path to the disclaimer/intro video."
            ),
        ),

        # ----------------------------------------------------
        # Target directory
        # ----------------------------------------------------

        "target_directory": Param(
            default="",
            type="string",
            title="Target Directory Location",
            description=(
                "Directory where the generated video directory "
                "will be created."
            ),
        ),

        # ----------------------------------------------------
        # Video context
        # ----------------------------------------------------

        "video_context": Param(
            default="",
            type="string",
            title="Comma Separated Video Context String",
            description=(
                "Enter the video context. "
            ),
        ),

        # ----------------------------------------------------
        # YouTube URL
        # ----------------------------------------------------

        "video_url": Param(
            default="",
            type="string",
            title="YouTube Video URL",
            description="Enter the original YouTube video URL.",
        ),

        # ----------------------------------------------------
        # Start time
        # ----------------------------------------------------

        "start_time": Param(
            default="00:00:00",
            type="string",
            title="Video Start Time",
            description="Enter start time in HH:MM:SS.",
        ),

        # ----------------------------------------------------
        # End time
        # ----------------------------------------------------

        "end_time": Param(
            default="00:00:30",
            type="string",
            title="Video End Time",
            description="Enter end time in HH:MM:SS.",
        ),
    },
) as dag:

    @task
    def process_youtube_clip(**context):

        # ====================================================
        # 1. Read Airflow parameters
        # ====================================================

        params = context["params"]

        disclaimer_location = params["disclaimer_location"]
        target_directory = params["target_directory"]
        video_context = params["video_context"]
        video_url = params["video_url"]
        start_time = params["start_time"]
        end_time = params["end_time"]

        # ====================================================
        # 2. Basic validation
        # ====================================================

        if not disclaimer_location:
            raise ValueError(
                "disclaimer_location cannot be empty."
            )

        if not target_directory:
            raise ValueError(
                "target_directory cannot be empty."
            )

        if not video_url:
            raise ValueError(
                "video_url cannot be empty."
            )

        if not video_context:
            raise ValueError(
                "video_context cannot be empty."
            )

        if not os.path.isfile(disclaimer_location):
            raise FileNotFoundError(
                f"Disclaimer video does not exist: "
                f"{disclaimer_location}"
            )

        # ====================================================
        # 3. Parse timestamps
        # ====================================================

        start_seconds = timestamp_to_seconds(start_time)
        end_seconds = timestamp_to_seconds(end_time)

        if start_seconds >= end_seconds:
            raise ValueError(
                "Start time must be earlier than end time."
            )

        # ====================================================
        # 4. Extract YouTube video ID
        # ====================================================

        video_id = get_video_id(video_url)

        # ====================================================
        # 5. Create unique output directory
        #
        # Example:
        #
        # 3yo23GvS7io_01_17_27_01_58_45_20260926_220530
        # ====================================================

        safe_start = safe_timestamp_for_directory(start_time)
        safe_end = safe_timestamp_for_directory(end_time)

        generation_timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        output_directory_name = (
            f"{video_id}_"
            f"{safe_start}_"
            f"{safe_end}_"
            f"{generation_timestamp}"
        )

        output_directory = (
            Path(target_directory) /
            output_directory_name
        )

        output_directory.mkdir(
            parents=True,
            exist_ok=False,
        )

        print(
            f"Output directory: {output_directory}"
        )

        # ====================================================
        # 6. Temporary download location
        # ====================================================

        download_template = str(
            output_directory /
            f"{video_id}_download.%(ext)s"
        )

        # ====================================================
        # 7. Download YouTube clip
        # ====================================================

        print(
            f"Downloading YouTube clip "
            f"{start_time} -> {end_time}"
        )

        download_options = {

            "format": "bestvideo+bestaudio/best",

            "outtmpl": download_template,

            "merge_output_format": "mp4",

            "download_ranges": download_range_func(
                None,
                [
                    (
                        start_seconds,
                        end_seconds,
                    )
                ],
            ),

            "force_keyframes_at_cuts": True,

            "noplaylist": True,
        }

        with yt_dlp.YoutubeDL(download_options) as ydl:

            info = ydl.extract_info(
                video_url,
                download=True,
            )

            downloaded_file = ydl.prepare_filename(info)

        # ====================================================
        # 8. Determine actual downloaded MP4
        # ====================================================

        youtube_file = (
            os.path.splitext(downloaded_file)[0]
            + ".mp4"
        )

        if not os.path.exists(youtube_file):

            youtube_file = downloaded_file

        if not os.path.exists(youtube_file):
            raise FileNotFoundError(
                "yt-dlp completed but the downloaded "
                f"file could not be found: {youtube_file}"
            )

        # ====================================================
        # 9. Rename initial downloaded clip
        #
        # This is the ORIGINAL downloaded clip before
        # the disclaimer is added.
        # ====================================================

        initial_file = (
            output_directory /
            f"{video_id}_{safe_start}_{safe_end}_INITIAL.mp4"
        )

        shutil.move(
            youtube_file,
            initial_file,
        )

        print(
            f"Initial file: {initial_file}"
        )

        # ====================================================
        # 10. Final output filename
        # ====================================================

        final_file = (
            output_directory /
            f"{video_id}_FINAL_1080p60.mp4"
        )

        # ====================================================
        # 11. FFmpeg:
        #
        # Disclaimer + YouTube clip
        # -> 1920x1080
        # -> 60 FPS
        # -> H.264
        # -> AAC
        # ====================================================

        ffmpeg_command = [

            "ffmpeg",

            "-y",

            "-i",
            str(disclaimer_location),

            "-i",
            str(initial_file),

            "-filter_complex",

            (
                # --------------------------------------------
                # Intro video
                # --------------------------------------------

                "[0:v]"
                "scale=1920:1080:"
                "force_original_aspect_ratio=decrease,"
                "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,"
                "fps=60,"
                "setsar=1"
                "[intro_v];"

                # --------------------------------------------
                # Main YouTube video
                # --------------------------------------------

                "[1:v]"
                "scale=1920:1080:"
                "force_original_aspect_ratio=decrease,"
                "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,"
                "fps=60,"
                "setsar=1"
                "[main_v];"

                # --------------------------------------------
                # Intro audio
                # --------------------------------------------

                "[0:a]"
                "aresample=48000,"
                "aformat="
                "sample_fmts=fltp:"
                "sample_rates=48000:"
                "channel_layouts=stereo"
                "[intro_a];"

                # --------------------------------------------
                # Main audio
                # --------------------------------------------

                "[1:a]"
                "aresample=48000,"
                "aformat="
                "sample_fmts=fltp:"
                "sample_rates=48000:"
                "channel_layouts=stereo"
                "[main_a];"

                # --------------------------------------------
                # Concatenate intro + main video
                # --------------------------------------------

                "[intro_v][intro_a]"
                "[main_v][main_a]"
                "concat=n=2:v=1:a=1"
                "[v][a]"
            ),

            "-map",
            "[v]",

            "-map",
            "[a]",

            "-c:v",
            "libx264",

            "-preset",
            "medium",

            "-crf",
            "18",

            "-r",
            "60",

            "-pix_fmt",
            "yuv420p",

            "-c:a",
            "aac",

            "-b:a",
            "192k",

            "-movflags",
            "+faststart",

            str(final_file),
        ]

        print("Starting FFmpeg rendering...")

        subprocess.run(
            ffmpeg_command,
            check=True,
        )

        if not final_file.exists():
            raise FileNotFoundError(
                f"FFmpeg completed but final file "
                f"was not found: {final_file}"
            )

        # ====================================================
        # 12. Generate ChatGPT prompts
        # ====================================================

        prompts = create_chatgpt_prompts(
            video_context=video_context,
            video_url=video_url,
            end_time=end_time,
        )

        prompts_file = (
            output_directory /
            "CHATGPT_PROMPTS.txt"
        )

        prompts_file.write_text(
            prompts,
            encoding="utf-8",
        )

        # ====================================================
        # 13. Save metadata
        # ====================================================

        metadata_file = (
            output_directory /
            "VIDEO_METADATA.txt"
        )

        metadata = f"""
VIDEO ID
========
{video_id}

VIDEO URL
=========
{video_url}

START TIME
==========
{start_time}

END TIME
========
{end_time}

VIDEO CONTEXT
=============
{video_context}

GENERATION TIMESTAMP
====================
{generation_timestamp}

OUTPUT DIRECTORY
================
{output_directory}

INITIAL FILE
============
{initial_file.name}

FINAL FILE
==========
{final_file.name}

CHATGPT PROMPTS
===============
{prompts_file.name}
""".strip()

        metadata_file.write_text(
            metadata,
            encoding="utf-8",
        )

        # ====================================================
        # 14. Remove any temporary download leftovers
        # ====================================================

        for file in output_directory.iterdir():

            if file.name.endswith(".part"):
                file.unlink(missing_ok=True)

            elif file.name.endswith(".ytdl"):
                file.unlink(missing_ok=True)

        # ====================================================
        # 15. Print result
        # ====================================================

        print("")
        print("=" * 70)
        print("DOWNLOAD AND RENDERING COMPLETE")
        print("=" * 70)

        print(f"Video ID       : {video_id}")
        print(f"Start time     : {start_time}")
        print(f"End time       : {end_time}")
        print(f"Output dir     : {output_directory}")
        print(f"Initial file   : {initial_file}")
        print(f"Final file     : {final_file}")
        print(f"Prompts file   : {prompts_file}")
        print(f"Metadata file  : {metadata_file}")
        print("=" * 70)

        # ====================================================
        # 16. Return useful information to Airflow XCom
        # ====================================================

        return {
            "video_id": video_id,
            "start_time": start_time,
            "end_time": end_time,
            "output_directory": str(output_directory),
            "initial_file": str(initial_file),
            "final_file": str(final_file),
            "prompts_file": str(prompts_file),
            "metadata_file": str(metadata_file),
        }

    process_youtube_clip()