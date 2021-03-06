/*  Copyright 2019-2020 the Conwayste Developers.
 *
 *  This file is part of conwayste.
 *
 *  conwayste is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  conwayste is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with conwayste.  If not, see
 *  <http://www.gnu.org/licenses/>. */

use std::collections::HashMap;
use std::error::Error;

use ggez::graphics::{Font, Rect};
use ggez::mint::Point2;
use ggez::Context;

use id_tree::NodeId;

use crate::config::Config;
use crate::constants;
use crate::ui::{
    color_with_alpha, common, context, Button, Chatbox, Checkbox, GameArea, InsertLocation, Label, Layering, Pane,
    TextField, UIResult, Widget,
};
use crate::Screen;

use chromatica::css;
use context::{
    EmitEvent, // so we can call .on(...) on widgets that implement this
    EventType,
};

// When adding support for a new widget, use this macro to define a routine which allows the
// developer to search in a `UILayout`/`Screen` pair for a widget by its ID. In most cases this is
// all you need to retrieve a widget for mutating, like on an "update".
macro_rules! add_widget_from_screen_id_mut {
    ($type:ident) => {
        #[allow(unused)]
        impl<'a> $type {
            pub fn widget_from_screen_and_id_mut(
                ui: &'a mut UILayout,
                screen: Screen,
                id: &'a NodeId,
            ) -> crate::ui::UIResult<&'a mut $type> {
                if let Some(layer) = ui.get_screen_layering_mut(screen) {
                    return $type::widget_from_id_mut(layer, id);
                }
                Err(Box::new(crate::ui::UIError::InvalidArgument {
                    reason: format!("{:?} not found in UI Layout", screen),
                }))
            }
        }
    };
}

// This macro is similar to `add_widget_from_screen_and_id_mut!()` however it creates a non-mutable
// function. This should be used when you do not need to mutate the underlying widget, like when
// drawing a widget to the screen.
macro_rules! add_widget_from_screen_id {
    ($type:ident) => {
        #[allow(unused)]
        impl<'a> $type {
            pub fn widget_from_screen_and_id(
                ui: &'a UILayout,
                screen: Screen,
                id: &'a NodeId,
            ) -> crate::ui::UIResult<&'a $type> {
                if let Some(layer) = ui.get_screen_layering(screen) {
                    return $type::widget_from_id(layer, id);
                }
                Err(Box::new(crate::ui::UIError::InvalidArgument {
                    reason: format!("{:?} not found in UI Layout", screen),
                }))
            }
        }
    };
}

pub struct UILayout {
    pub layers: HashMap<Screen, Layering>,
}

pub struct StaticNodeIds {
    // HACK
    // The fields below correspond to static ui elements that the client may need to interact with
    // regardless of what is displayed on screen. For example, new chat messages should always be
    // forwarded to the UI widget.
    pub chatbox_id:      NodeId,
    pub chatbox_pane_id: NodeId,
    pub chatbox_tf_id:   NodeId,
    pub game_area_id:    NodeId,
}

/// `UILayout` is responsible for the definition and storage of UI elements.
impl UILayout {
    pub fn get_screen_layering(&self, screen: Screen) -> Option<&Layering> {
        self.layers.get(&screen)
    }

    /// Get all layers associated with the specified Screen
    pub fn get_screen_layering_mut(&mut self, screen: Screen) -> Option<&mut Layering> {
        self.layers.get_mut(&screen)
    }

    fn build_options_menu(
        ctx: &mut Context,
        config: &Config,
        default_font_info: common::FontInfo,
    ) -> UIResult<Layering> {
        let mut layer_options = Layering::new();
        let mut fullscreen_checkbox = Box::new(Checkbox::new(
            ctx,
            config.get().video.fullscreen,
            default_font_info,
            "Toggle FullScreen".to_owned(),
            Rect::new(10.0, 210.0, 20.0, 20.0),
        ));

        let name_color = color_with_alpha(css::WHITE, 1.0);
        let value_color = color_with_alpha(css::AQUAMARINE, 1.0);
        layer_options.add_widget(
            Box::new(Label::new(
                ctx,
                default_font_info,
                "Resolution".to_owned(),
                name_color,
                Point2 { x: 10.0, y: 300.0 },
            )),
            InsertLocation::AtCurrentLayer,
        )?;

        let mut resolution_value_label = Box::new(Label::new(
            ctx,
            default_font_info,
            "<no data>".to_owned(),
            value_color,
            Point2 { x: 200.0, y: 300.0 },
        ));
        resolution_value_label
            .on(context::EventType::Update, Box::new(resolution_update_handler))
            .unwrap();
        layer_options.add_widget(resolution_value_label, InsertLocation::AtCurrentLayer)?;

        // unwrap OK here because we are not calling .on from within a handler
        fullscreen_checkbox
            .on(EventType::Click, Box::new(fullscreen_toggle_handler))
            .unwrap();
        layer_options.add_widget(fullscreen_checkbox, InsertLocation::AtCurrentLayer)?;

        let playername_label = Box::new(Label::new(
            ctx,
            default_font_info,
            "Player Name:".to_owned(),
            name_color,
            Point2 { x: 0.0, y: 0.0 },
        ));
        let pnlabel_x = playername_label.position().x;
        let pnlabel_y = playername_label.position().y;
        let pnlabel_r_edge = playername_label.size().0 + pnlabel_x;
        let mut playername_tf = Box::new(TextField::new(
            default_font_info,
            Rect::new(pnlabel_r_edge + 20.0, pnlabel_y, 200.0, 30.0),
        ));
        playername_tf.on(EventType::Load, Box::new(load_player_name)).unwrap();
        playername_tf.on(EventType::Save, Box::new(save_player_name)).unwrap();

        let mut playername_pane = Box::new(Pane::new(Rect::new(10.0, 0.0, 0.0, 0.0)));
        playername_pane.set_rect(Rect::new(
            10.0,
            400.0,
            playername_label.size().0 + playername_tf.size().0,
            f32::max(playername_label.size().1, playername_tf.size().1),
        ))?;
        playername_pane.border = 0.0;

        let playername_pane_id = layer_options.add_widget(playername_pane, InsertLocation::AtCurrentLayer)?;
        layer_options.add_widget(playername_label, InsertLocation::ToNestedContainer(&playername_pane_id))?;
        layer_options.add_widget(playername_tf, InsertLocation::ToNestedContainer(&playername_pane_id))?;

        Ok(layer_options)
    }

    fn build_main_menu(ctx: &mut Context, default_font_info: common::FontInfo) -> UIResult<Layering> {
        let mut layer_mainmenu = Layering::new();

        // Create a new pane, and add two test buttons to it.
        let pane = Box::new(Pane::new(Rect::new_i32(20, 20, 410, 450)));
        let mut serverlist_button = Box::new(Button::new(ctx, default_font_info, "Server List".to_owned()));
        serverlist_button.set_rect(Rect::new(10.0, 10.0, 180.0, 50.0))?;
        serverlist_button
            .on(EventType::Click, Box::new(server_list_click_handler))
            .unwrap(); // unwrap OK

        let mut start_1p_game_button = Box::new(Button::new(
            ctx,
            default_font_info,
            "Start Single Player Game".to_owned(),
        ));
        start_1p_game_button.set_rect(Rect::new(10.0, 70.0, 350.0, 50.0))?;
        start_1p_game_button
            .on(EventType::Click, Box::new(start_or_resume_game_click_handler))
            .unwrap(); // unwrap OK

        let mut options_button = Box::new(Button::new(ctx, default_font_info, "Options".to_owned()));
        options_button.set_rect(Rect::new(10.0, 130.0, 180.0, 50.0))?;
        options_button
            .on(EventType::Click, Box::new(options_click_handler))
            .unwrap(); // unwrap OK

        let mut quit_button = Box::new(Button::new(ctx, default_font_info, "Quit".to_owned()));
        quit_button.set_rect(Rect::new(10.0, 190.0, 180.0, 50.0))?;
        quit_button.on(EventType::Click, Box::new(quit_click_handler)).unwrap(); // unwrap OK

        let menupane_id = layer_mainmenu.add_widget(pane, InsertLocation::AtCurrentLayer)?;
        // Add widgets in the order you want keyboard focus
        layer_mainmenu.add_widget(serverlist_button, InsertLocation::ToNestedContainer(&menupane_id))?;
        layer_mainmenu.add_widget(start_1p_game_button, InsertLocation::ToNestedContainer(&menupane_id))?;
        layer_mainmenu.add_widget(options_button, InsertLocation::ToNestedContainer(&menupane_id))?;
        layer_mainmenu.add_widget(quit_button, InsertLocation::ToNestedContainer(&menupane_id))?;
        Ok(layer_mainmenu)
    }

    pub fn new(ctx: &mut Context, config: &Config, font: Font) -> UIResult<(UILayout, StaticNodeIds)> {
        let mut ui_layers = HashMap::new();

        let default_font_info = common::FontInfo::new(ctx, font, None);

        let layer_mainmenu = UILayout::build_main_menu(ctx, default_font_info)?;
        debug!("MENU WIDGET TREE");
        layer_mainmenu.debug_display_widget_tree();
        ui_layers.insert(Screen::Menu, layer_mainmenu);

        let layer_options = UILayout::build_options_menu(ctx, config, default_font_info)?;
        debug!("OPTIONS WIDGET TREE");
        layer_options.debug_display_widget_tree();
        ui_layers.insert(Screen::Options, layer_options);

        // ==== In-Game (Run screen) ====
        let mut layer_ingame = Layering::new();
        let chat_pane_rect = *constants::DEFAULT_CHATBOX_RECT;
        let mut chatpane = Box::new(Pane::new(chat_pane_rect));
        chatpane.bg_color = Some(*constants::colors::CHAT_PANE_FILL_COLOR);
        let chatpane_id = layer_ingame.add_widget(chatpane, InsertLocation::AtCurrentLayer)?;

        let chatbox_rect = Rect::new(
            0.0,
            0.0,
            chat_pane_rect.w,
            chat_pane_rect.h - constants::CHAT_TEXTFIELD_HEIGHT,
        );
        let chatbox_font_info = common::FontInfo::new(ctx, font, Some(*constants::DEFAULT_CHATBOX_FONT_SCALE));
        let mut chatbox = Chatbox::new(chatbox_font_info, constants::CHATBOX_HISTORY);
        chatbox.set_rect(chatbox_rect)?;

        let chatbox = Box::new(chatbox);

        let textfield_rect = Rect::new(
            chatbox_rect.x,
            chatbox_rect.bottom(),
            chatbox_rect.w,
            constants::CHAT_TEXTFIELD_HEIGHT,
        );
        let mut textfield = Box::new(TextField::new(default_font_info, textfield_rect));
        textfield.bg_color = Some(*constants::colors::CHAT_PANE_FILL_COLOR);
        let chatbox_id = layer_ingame.add_widget(chatbox, InsertLocation::ToNestedContainer(&chatpane_id))?;
        let chatbox_tf_id = layer_ingame.add_widget(textfield, InsertLocation::ToNestedContainer(&chatpane_id))?;

        let mut game_area = Box::new(GameArea::new());
        info!("Setting Game Area to {:?}", config.get_resolution());
        let (x, y) = config.get_resolution();
        game_area.set_rect(Rect::new(0.0, 0.0, x, y))?;
        let game_area_id = layer_ingame.add_widget(game_area, InsertLocation::AtCurrentLayer)?;

        debug!("RUN WIDGET TREE");
        layer_ingame.debug_display_widget_tree();
        ui_layers.insert(Screen::Run, layer_ingame);

        Ok((
            UILayout { layers: ui_layers },
            StaticNodeIds {
                chatbox_id,
                chatbox_pane_id: chatpane_id,
                chatbox_tf_id,
                game_area_id,
            },
        ))
    }
}
fn fullscreen_toggle_handler(
    obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    use context::Handled::*;

    // NOTE: the checkbox installed its own handler to toggle the `enabled` field on click
    // We are running after it, since the handler registered first gets called first.

    let checkbox = obj.downcast_ref::<Checkbox>().unwrap();

    uictx.config.modify(|settings| {
        settings.video.fullscreen = checkbox.enabled;
    });
    Ok(Handled)
}

fn server_list_click_handler(
    _obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    uictx.push_screen(Screen::ServerList);
    Ok(context::Handled::Handled)
}

fn options_click_handler(
    _obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    uictx.push_screen(Screen::Options);
    Ok(context::Handled::Handled)
}

fn start_or_resume_game_click_handler(
    obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    let btn = obj.downcast_mut::<Button>().unwrap(); // unwrap OK because this is only registered on a button

    // TODO: don't do this anymore once we have an in-game menu that is above Screen::Run in screen_stack.
    btn.label.set_text(uictx.ggez_context, "Resume Game".to_owned());

    uictx.push_screen(Screen::Run);
    Ok(context::Handled::Handled)
}

fn quit_click_handler(
    _obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    info!("QUIT CLICKED");
    ggez::event::quit(uictx.ggez_context);
    Ok(context::Handled::Handled)
}

fn resolution_update_handler(
    obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    let label = obj.downcast_mut::<Label>().unwrap(); // unwrap OK because it's always a Label
    let (x, y) = (
        uictx.config.get().video.resolution_x,
        uictx.config.get().video.resolution_y,
    );
    let new_res_text = format!("{} x {}", x, y);
    if label.text() != new_res_text.as_str() {
        label.set_text(uictx.ggez_context, new_res_text);
    }
    Ok(context::Handled::Handled)
}

// TODO find a place for all these specific widget-instance handlers
fn load_player_name(
    obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    let textfield = obj.downcast_mut::<TextField>().unwrap(); // unwrap OK because it's always a textfield
    let ref player_name = uictx.config.get().user.name;
    textfield.set_text(player_name.clone());
    Ok(context::Handled::NotHandled)
}

fn save_player_name(
    obj: &mut dyn EmitEvent,
    uictx: &mut context::UIContext,
    _evt: &context::Event,
) -> Result<context::Handled, Box<dyn Error>> {
    let textfield = obj.downcast_mut::<TextField>().unwrap(); // unwrap OK because it's always a textfield
    if let Some(player_name) = textfield.text() {
        uictx.config.modify(|c| {
            c.user.name = player_name.clone();
        });
    }
    Ok(context::Handled::NotHandled)
}

add_widget_from_screen_id_mut!(Button);
add_widget_from_screen_id_mut!(Checkbox);
add_widget_from_screen_id_mut!(Label);
add_widget_from_screen_id_mut!(Pane);
add_widget_from_screen_id_mut!(TextField);
add_widget_from_screen_id_mut!(Chatbox);
add_widget_from_screen_id_mut!(GameArea);
add_widget_from_screen_id!(GameArea);
