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


def sanitize_filename(value: str, max_length: int = 150) -> str:
    """
    Convert any string into a strictly alphanumeric snake_case
    filesystem-safe name.

    Allowed characters:
        A-Z
        a-z
        0-9
        _

    Everything else is removed/replaced.

    Examples:

        "Why Does Allah Want This?! 😳"
        ->
        "Why_Does_Allah_Want_This"

        "Sahil vs. Muslim Speaker!"
        ->
        "Sahil_vs_Muslim_Speaker"
    """

    if not value:
        return "UNTITLED"

    # --------------------------------------------------------
    # Convert common separators to spaces
    # --------------------------------------------------------

    value = value.replace("-", " ")
    value = value.replace("–", " ")
    value = value.replace("—", " ")

    # --------------------------------------------------------
    # Remove anything that isn't ASCII alphanumeric
    # or whitespace.
    #
    # This deliberately removes:
    # emojis
    # punctuation
    # symbols
    # non-ASCII characters
    # --------------------------------------------------------

    value = re.sub(
        r"[^A-Za-z0-9\s]",
        " ",
        value,
    )

    # --------------------------------------------------------
    # Collapse whitespace
    # --------------------------------------------------------

    value = re.sub(
        r"\s+",
        " ",
        value,
    ).strip()

    # --------------------------------------------------------
    # Convert spaces to underscores
    # --------------------------------------------------------

    value = value.replace(" ", "_")

    # --------------------------------------------------------
    # Remove repeated underscores
    # --------------------------------------------------------

    value = re.sub(
        r"_+",
        "_",
        value,
    )

    # --------------------------------------------------------
    # Remove leading/trailing underscores
    # --------------------------------------------------------

    value = value.strip("_")

    # --------------------------------------------------------
    # Ensure only alphanumeric + underscore
    # --------------------------------------------------------

    value = re.sub(
        r"[^A-Za-z0-9_]",
        "",
        value,
    )

    # --------------------------------------------------------
    # Prevent excessively long filesystem names
    # --------------------------------------------------------

    value = value[:max_length]

    # --------------------------------------------------------
    # Remove trailing underscore after truncation
    # --------------------------------------------------------

    value = value.rstrip("_")

    if not value:
        return "UNTITLED"

    return value


def timestamp_for_youtube_url(timestamp: str) -> str:
    """
    Convert HH:MM:SS into YouTube timestamp format.

    Example:
        01:02:20 -> 1h02m20s
    """

    h, m, s = map(int, timestamp.split(":"))

    return f"{h}h{m:02d}m{s:02d}s"


def safe_timestamp_for_filename(timestamp: str) -> str:
    """
    Convert HH:MM:SS into strictly safe filename format.

    Example:
        01:17:27 -> 01_17_27
    """

    return timestamp.replace(":", "_")


def create_chatgpt_prompts(
    video_context: str,
    video_url: str,
    end_time: str,
) -> str:

    video_id = get_video_id(video_url)

    youtube_timestamp = timestamp_for_youtube_url(
        end_time
    )

    attribution_url = (
        f"https://www.youtube.com/watch?v={video_id}"
        f"&t={youtube_timestamp}"
    )

    prompt_1 = f"""
============================================================
PROMPT 1 - YOUTUBE TITLE DESCRIPTION AND TAGS
============================================================

You are an expert YouTube growth, SEO, CTR and audience
retention copywriter.

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
3. Make it attention-grabbing without inventing facts.
4. Generate a compelling YouTube description.
5. The FIRST line of the description MUST contain the
   attribution to the original upload:

   {attribution_url}

6. The description should clearly explain the context.
7. Make the writing natural and human rather than AI-generated.
8. Do not invent claims that are not supported by the context.
9. Generate YouTube tags.
10. Tags MUST be comma-separated.
11. Tags MUST be on ONE SINGLE LINE.
12. Tags MUST be 500 characters or fewer.
13. Optimize tags for YouTube search and discovery.
14. Avoid generic AI-looking tags.
15. Use natural phrases a real YouTube creator would use.

Return ONLY:

TITLE:
<one optimized title>

DESCRIPTION:
<optimized description>

TAGS:
<tag1, tag2, tag3, ...>
"""

    prompt_2 = f"""
============================================================
PROMPT 2 - HIGH CTR YOUTUBE THUMBNAIL
============================================================

You are an expert YouTube thumbnail designer specializing
in high-CTR debate, commentary and discussion videos.

Video context:

{video_context}

Create a detailed image-generation prompt for a YouTube
thumbnail.

THUMBNAIL REQUIREMENTS:

1. Exact canvas size: 1280x720 pixels.
2. 16:9 YouTube thumbnail composition.
3. The thumbnail must immediately communicate the central
   conflict or debate.
4. EX-EX-MUSLIM SAHIL must be on the LEFT side.
5. Sahil must be shown wearing his recognizable mask.
6. The DEBATER must be on the RIGHT side.
7. Use the supplied video context to identify the debater.
8. Both subjects should be large and clearly visible.
9. Use strong contrasting expressions and body language.
10. Use dramatic but realistic lighting.
11. Create strong visual separation between both sides.
12. Keep the composition clean and professional.
13. Any thumbnail text should be short and highly readable.
14. Do not overcrowd the thumbnail.
15. Do not add unnecessary logos or watermarks.
16. Do not exactly copy an existing thumbnail.
17. Make the design suitable for mobile viewing.
18. Keep faces and text away from the extreme edges.
19. Prioritize curiosity and visual storytelling.
20. Use information supported by the supplied context.

If the debater's identity is available in the context,
use that person on the right side.

Return ONE complete image-generation prompt.

Final image dimensions:

1280x720 pixels
"""

    return f"""
VIDEO CONTEXT
=============
{video_context}

ORIGINAL VIDEO URL
==================
{video_url}

END TIME
========
{end_time}

ATTRIBUTION URL
===============
{attribution_url}


{prompt_1}


{prompt_2}
""".strip()


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="yt_download_automation_runner",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "youtube",
        "yt-dlp",
        "ffmpeg",
        "video",
        "content-generation",
    ],

    params={

        "disclaimer_location": Param(
            default="/DATA/Media/programs_and_routines/yt_crop_download/assets/channel_disclaimer_4k.mp4",
            type="string",
            title="Disclaimer Video Location",
            description="Full path to disclaimer/intro video.",
        ),

        "target_directory": Param(
            default="/DATA/Media/programs_and_routines/yt_crop_download/outbound",
            type="string",
            title="Target Directory Location",
            description="Base directory for generated output.",
        ),

        "video_context": Param(
            default="good video, very good video",
            type="string",
            title="Video Context",
            description="Enter the video context.",
        ),

        "video_url": Param(
            default="https://www.youtube.com/watch?v=QDia3e12czc",
            type="string",
            title="YouTube Video URL",
            description="Enter the original YouTube video URL.",
        ),

        "start_time": Param(
            default="00:00:00",
            type="string",
            title="Video Start Time",
            description="Enter start time in HH:MM:SS.",
        ),

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

        params = context["params"]

        disclaimer_location = params["disclaimer_location"]
        target_directory = params["target_directory"]
        video_context = params["video_context"]
        video_url = params["video_url"]
        start_time = params["start_time"]
        end_time = params["end_time"]

        # ====================================================
        # Validate inputs
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
        # Validate timestamps
        # ====================================================

        start_seconds = timestamp_to_seconds(start_time)
        end_seconds = timestamp_to_seconds(end_time)

        if start_seconds >= end_seconds:
            raise ValueError(
                "Start time must be earlier than end time."
            )

        # ====================================================
        # Get YouTube video ID
        # ====================================================

        video_id = get_video_id(video_url)

        # ====================================================
        # Get YouTube video title
        #
        # IMPORTANT:
        # Do this BEFORE creating the output directory because
        # the title is now the basis of the directory/file name.
        # ====================================================

        print("Getting YouTube video information...")

        with yt_dlp.YoutubeDL({
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
        }) as ydl:

            video_info = ydl.extract_info(
                video_url,
                download=False,
            )

        original_video_title = (
            video_info.get("title")
            or "UNTITLED_VIDEO"
        )

        # ====================================================
        # Sanitize title
        # ====================================================

        safe_video_title = sanitize_filename(
            original_video_title
        )

        print(
            f"Original title: {original_video_title}"
        )

        print(
            f"Sanitized title: {safe_video_title}"
        )

        # ====================================================
        # Safe timestamps
        # ====================================================

        safe_start = safe_timestamp_for_filename(
            start_time
        )

        safe_end = safe_timestamp_for_filename(
            end_time
        )

        # ====================================================
        # Generation timestamp
        #
        # YYYYMMDD_HHMMSS
        # ====================================================

        generation_timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        # ====================================================
        # Output directory name
        #
        # ONLY:
        # A-Z
        # a-z
        # 0-9
        # _
        #
        # Example:
        #
        # Why_Does_Allah_Want_A_Test_Sahil_01_17_27_01_58_45_20260926_220530
        # ====================================================

        output_directory_name = (
            f"{safe_video_title}_"
            f"{safe_start}_"
            f"{safe_end}_"
            f"{generation_timestamp}"
        )

        # Final safety pass
        output_directory_name = sanitize_filename(
            output_directory_name,
            max_length=220,
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
        # File prefix
        # ====================================================

        file_prefix = (
            f"{safe_video_title}_"
            f"{safe_start}_"
            f"{safe_end}"
        )

        # ====================================================
        # Download location
        # ====================================================

        download_template = str(
            output_directory /
            f"{file_prefix}_DOWNLOAD.%(ext)s"
        )

        # ====================================================
        # Download YouTube clip
        # ====================================================

        print(
            f"Downloading: "
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

        with yt_dlp.YoutubeDL(
            download_options
        ) as ydl:

            info = ydl.extract_info(
                video_url,
                download=True,
            )

            downloaded_file = ydl.prepare_filename(
                info
            )

        # ====================================================
        # Determine downloaded MP4
        # ====================================================

        youtube_file = (
            os.path.splitext(downloaded_file)[0]
            + ".mp4"
        )

        if not os.path.exists(youtube_file):
            youtube_file = downloaded_file

        if not os.path.exists(youtube_file):
            raise FileNotFoundError(
                f"Downloaded video could not be found: "
                f"{youtube_file}"
            )

        # ====================================================
        # Initial file
        # ====================================================

        initial_file = (
            output_directory /
            f"{file_prefix}_INITIAL.mp4"
        )

        shutil.move(
            youtube_file,
            initial_file,
        )

        # ====================================================
        # Final file
        # ====================================================

        final_file = (
            output_directory /
            f"{file_prefix}_FINAL_1080P60.mp4"
        )

        # ====================================================
        # FFmpeg rendering
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
                "[0:v]"
                "scale=1920:1080:"
                "force_original_aspect_ratio=decrease,"
                "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,"
                "fps=60,"
                "setsar=1"
                "[intro_v];"

                "[1:v]"
                "scale=1920:1080:"
                "force_original_aspect_ratio=decrease,"
                "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,"
                "fps=60,"
                "setsar=1"
                "[main_v];"

                "[0:a]"
                "aresample=48000,"
                "aformat="
                "sample_fmts=fltp:"
                "sample_rates=48000:"
                "channel_layouts=stereo"
                "[intro_a];"

                "[1:a]"
                "aresample=48000,"
                "aformat="
                "sample_fmts=fltp:"
                "sample_rates=48000:"
                "channel_layouts=stereo"
                "[main_a];"

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

        print("Rendering final video...")

        subprocess.run(
            ffmpeg_command,
            check=True,
        )

        # ====================================================
        # Verify final file
        # ====================================================

        if not final_file.exists():
            raise FileNotFoundError(
                f"Final file was not created: {final_file}"
            )

        # ====================================================
        # Generate ChatGPT prompts
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
        # Metadata
        # ====================================================

        metadata_file = (
            output_directory /
            "VIDEO_METADATA.txt"
        )

        metadata = f"""
ORIGINAL VIDEO TITLE
====================
{original_video_title}

SANITIZED VIDEO TITLE
=====================
{safe_video_title}

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
        # Remove temporary files
        # ====================================================

        for file in output_directory.iterdir():

            if file.name.endswith(".part"):
                file.unlink(missing_ok=True)

            elif file.name.endswith(".ytdl"):
                file.unlink(missing_ok=True)

            elif "_DOWNLOAD." in file.name:
                file.unlink(missing_ok=True)

        # ====================================================
        # Final result
        # ====================================================

        print("")
        print("=" * 70)
        print("DOWNLOAD AND RENDERING COMPLETE")
        print("=" * 70)

        print(f"Original title : {original_video_title}")
        print(f"Safe title     : {safe_video_title}")
        print(f"Video ID       : {video_id}")
        print(f"Start          : {start_time}")
        print(f"End            : {end_time}")
        print(f"Output dir     : {output_directory}")
        print(f"Initial file   : {initial_file}")
        print(f"Final file     : {final_file}")
        print(f"Prompts        : {prompts_file}")
        print(f"Metadata       : {metadata_file}")

        print("=" * 70)

        return {
            "video_id": video_id,
            "original_title": original_video_title,
            "sanitized_title": safe_video_title,
            "start_time": start_time,
            "end_time": end_time,
            "output_directory": str(output_directory),
            "initial_file": str(initial_file),
            "final_file": str(final_file),
            "prompts_file": str(prompts_file),
            "metadata_file": str(metadata_file),
        }

    process_youtube_clip()