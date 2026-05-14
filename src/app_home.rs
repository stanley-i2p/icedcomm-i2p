use crate::app::{BackupOperation, Message};
use iced::widget::{button, checkbox, column, container, row, scrollable, text, text_input};
use iced::{Alignment, Background, Color, Element, Length, border};
use std::path::Path;

pub fn app_home_view<'a>(
    _show_logs: bool,
    sam_host_input: &'a str,
    sam_port_input: &'a str,
    sam_status: &'a str,
    sam_test_in_flight: bool,
    backup_export_passphrase: &'a str,
    backup_export_status: &'a str,
    backup_export_include_files: bool,
    backup_import_passphrase: &'a str,
    backup_import_status: &'a str,
    backup_import_restore_files: bool,
    pending_import_path: Option<&'a Path>,
    wipe_all_passphrase: &'a str,
    wipe_all_status: &'a str,
    profile_export_passphrase: &'a str,
    profile_export_status: &'a str,
    selected_profile_name: Option<&'a str>,
    profile_import_passphrase: &'a str,
    profile_import_status: &'a str,
    pending_profile_import_path: Option<&'a Path>,
    pending_profile_import_name: Option<&'a str>,
    backup_operation: BackupOperation,
) -> Element<'a, Message> {
    let sam_block = sam_operation_block(
        column![
            row![
                text("SAM").size(16).width(70),
                text_input("Host", sam_host_input)
                    .on_input_maybe((!sam_test_in_flight).then_some(Message::SamHostInputChanged))
                    .padding(10)
                    .size(13)
                    .width(220),
                text_input("Port", sam_port_input)
                    .on_input_maybe((!sam_test_in_flight).then_some(Message::SamPortInputChanged))
                    .on_submit_maybe(
                        (!sam_test_in_flight).then_some(Message::SaveSamSettingsPressed)
                    )
                    .padding(10)
                    .size(13)
                    .width(90),
                button(text("Save").size(13))
                    .padding([6, 10])
                    .style(crate::app::app_button_style)
                    .on_press_maybe(
                        (!sam_test_in_flight).then_some(Message::SaveSamSettingsPressed)
                    ),
                button(text("Test").size(13))
                    .padding([6, 10])
                    .style(crate::app::app_button_style)
                    .on_press_maybe((!sam_test_in_flight).then_some(Message::TestSamPressed)),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
            text(sam_status).size(12),
        ]
        .spacing(8)
        .width(Length::Fill),
    );

    let can_edit_export = backup_operation == BackupOperation::Idle;
    let can_edit_import = backup_operation == BackupOperation::Idle;
    let can_edit_wipe = backup_operation == BackupOperation::Idle;
    let can_edit_profile_export = backup_operation == BackupOperation::Idle;
    let can_edit_profile_import = backup_operation == BackupOperation::Idle;
    let can_confirm_import = backup_operation == BackupOperation::AwaitingReplaceConfirm;
    let can_confirm_wipe = backup_operation == BackupOperation::AwaitingWipeConfirm;
    let can_confirm_profile_import =
        backup_operation == BackupOperation::AwaitingProfileReplaceConfirm;

    let import_confirm = if let Some(path) = pending_import_path {
        row![
            text(format!(
                "Replace local profiles/files with {}?",
                path.display()
            ))
            .size(12)
            .width(Length::Fill),
            button(text("OK").size(12))
                .padding([6, 10])
                .style(crate::app::app_button_style)
                .on_press_maybe(
                    can_confirm_import.then_some(Message::BackupImportReplaceConfirmed)
                ),
            button(text("Cancel").size(12))
                .padding([6, 10])
                .style(crate::app::app_button_style)
                .on_press_maybe(
                    can_confirm_import.then_some(Message::BackupImportReplaceCancelled)
                ),
        ]
        .spacing(8)
        .align_y(Alignment::Center)
    } else {
        row![]
    };

    let wipe_confirm = if can_confirm_wipe {
        row![
            text("Delete all profiles and stored files?")
                .size(12)
                .width(Length::Fill),
            button(text("OK").size(12))
                .padding([6, 10])
                .style(crate::app::app_button_style)
                .on_press(Message::WipeAllConfirmed),
            button(text("Cancel").size(12))
                .padding([6, 10])
                .style(crate::app::app_button_style)
                .on_press(Message::WipeAllCancelled),
        ]
        .spacing(8)
        .align_y(Alignment::Center)
    } else {
        row![]
    };

    let wipe_block = danger_operation_block(
        column![
            row![
                button(text("Wipe All").size(13))
                    .padding([6, 10])
                    .style(crate::app::app_button_style)
                    .on_press_maybe(can_edit_wipe.then_some(Message::WipeAllPressed)),
                text_input("Unlock passphrase...", wipe_all_passphrase)
                    .on_input_maybe(can_edit_wipe.then_some(Message::WipeAllPassphraseChanged))
                    .on_submit_maybe(can_edit_wipe.then_some(Message::WipeAllPressed))
                    .secure(true)
                    .padding(10)
                    .size(13)
                    .width(260),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
            text(wipe_all_status).size(12),
            wipe_confirm,
        ]
        .spacing(8)
        .width(Length::Fill),
    );

    let profile_import_confirm = if let (Some(path), Some(profile_name)) =
        (pending_profile_import_path, pending_profile_import_name)
    {
        row![
            text(format!(
                "Replace local profile {profile_name} with {}?",
                path.display()
            ))
            .size(12)
            .width(Length::Fill),
            button(text("OK").size(12))
                .padding([6, 10])
                .style(crate::app::app_button_style)
                .on_press_maybe(
                    can_confirm_profile_import
                        .then_some(Message::ProfileBackupImportReplaceConfirmed)
                ),
            button(text("Cancel").size(12))
                .padding([6, 10])
                .style(crate::app::app_button_style)
                .on_press_maybe(
                    can_confirm_profile_import
                        .then_some(Message::ProfileBackupImportReplaceCancelled)
                ),
        ]
        .spacing(8)
        .align_y(Alignment::Center)
    } else {
        row![]
    };

    let export_block = operation_block(
        column![
            row![
                button(text("Export Backup").size(13))
                    .padding([6, 10])
                    .style(crate::app::app_button_style)
                    .on_press_maybe(can_edit_export.then_some(Message::ExportBackupPressed)),
                text_input("Export passphrase...", backup_export_passphrase)
                    .on_input_maybe(
                        can_edit_export.then_some(Message::BackupExportPassphraseChanged)
                    )
                    .on_submit_maybe(can_edit_export.then_some(Message::ExportBackupPressed))
                    .secure(true)
                    .padding(10)
                    .size(13)
                    .width(260),
                checkbox(backup_export_include_files)
                    .label("Include files")
                    .on_toggle_maybe(
                        can_edit_export.then_some(Message::BackupExportIncludeFilesChanged)
                    )
                    .text_size(12)
                    .width(Length::Shrink),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
            text(backup_export_status).size(12),
        ]
        .spacing(8)
        .width(Length::Fill),
    );

    let import_block = operation_block(
        column![
            row![
                button(text("Import Backup").size(13))
                    .padding([6, 10])
                    .style(crate::app::app_button_style)
                    .on_press_maybe(can_edit_import.then_some(Message::ImportBackupPressed)),
                text_input("Import passphrase...", backup_import_passphrase)
                    .on_input_maybe(
                        can_edit_import.then_some(Message::BackupImportPassphraseChanged)
                    )
                    .on_submit_maybe(can_edit_import.then_some(Message::ImportBackupPressed))
                    .secure(true)
                    .padding(10)
                    .size(13)
                    .width(260),
                checkbox(backup_import_restore_files)
                    .label("Restore files")
                    .on_toggle_maybe(
                        can_edit_import.then_some(Message::BackupImportRestoreFilesChanged)
                    )
                    .text_size(12)
                    .width(Length::Shrink),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
            text(backup_import_status).size(12),
            import_confirm,
        ]
        .spacing(8)
        .width(Length::Fill),
    );

    let selected_profile_text = selected_profile_name
        .map(|name| format!("Selected profile: {name}"))
        .unwrap_or_else(|| "Selected profile: none".into());

    let profile_export_block = operation_block(
        column![
            row![
                button(text("Export Profile").size(13))
                    .padding([6, 10])
                    .style(crate::app::app_button_style)
                    .on_press_maybe(
                        can_edit_profile_export.then_some(Message::ExportProfileBackupPressed)
                    ),
                text_input("Profile export passphrase...", profile_export_passphrase)
                    .on_input_maybe(
                        can_edit_profile_export.then_some(Message::ProfileExportPassphraseChanged)
                    )
                    .on_submit_maybe(
                        can_edit_profile_export.then_some(Message::ExportProfileBackupPressed)
                    )
                    .secure(true)
                    .padding(10)
                    .size(13)
                    .width(260),
                text(selected_profile_text).size(12).width(Length::Fill),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
            text(profile_export_status).size(12),
        ]
        .spacing(8)
        .width(Length::Fill),
    );

    let profile_import_block = operation_block(
        column![
            row![
                button(text("Import Profile").size(13))
                    .padding([6, 10])
                    .style(crate::app::app_button_style)
                    .on_press_maybe(
                        can_edit_profile_import.then_some(Message::ImportProfileBackupPressed)
                    ),
                text_input("Profile import passphrase...", profile_import_passphrase)
                    .on_input_maybe(
                        can_edit_profile_import.then_some(Message::ProfileImportPassphraseChanged)
                    )
                    .on_submit_maybe(
                        can_edit_profile_import.then_some(Message::ImportProfileBackupPressed)
                    )
                    .secure(true)
                    .padding(10)
                    .size(13)
                    .width(260),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
            text(profile_import_status).size(12),
            profile_import_confirm,
        ]
        .spacing(8)
        .width(Length::Fill),
    );

    let actions = container(
        column![
            sam_block,
            wipe_block,
            export_block,
            import_block,
            profile_export_block,
            profile_import_block,
        ]
        .spacing(14)
        .width(Length::Fill),
    )
    .padding(16)
    .width(Length::Fill);

    container(
        scrollable(column![actions].spacing(12).width(Length::Fill))
            .width(Length::Fill)
            .height(Length::Fill),
    )
    .width(Length::Fill)
    .height(Length::Fill)
    .into()
}

fn operation_block<'a>(content: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    container(content)
        .padding(12)
        .width(Length::Fill)
        .style(|_| container::Style {
            background: Some(Background::Color(Color::from_rgb8(24, 24, 30))),
            border: border::Border {
                color: Color::from_rgb8(64, 64, 72),
                width: 1.0,
                radius: border::Radius::from(6.0),
            },
            ..Default::default()
        })
        .into()
}

fn sam_operation_block<'a>(content: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    container(content)
        .padding(12)
        .width(Length::Fill)
        .style(|_| container::Style {
            background: Some(Background::Color(Color::from_rgb8(24, 24, 30))),
            border: border::Border {
                color: Color::from_rgb8(0x58, 0x65, 0xF2),
                width: 2.0,
                radius: border::Radius::from(6.0),
            },
            ..Default::default()
        })
        .into()
}

fn danger_operation_block<'a>(content: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    container(content)
        .padding(12)
        .width(Length::Fill)
        .style(|_| container::Style {
            background: Some(Background::Color(Color::from_rgb8(34, 20, 22))),
            border: border::Border {
                color: Color::from_rgb8(170, 45, 45),
                width: 1.5,
                radius: border::Radius::from(6.0),
            },
            ..Default::default()
        })
        .into()
}
